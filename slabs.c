/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Slabs memory allocation, based on powers-of-N. Slabs are up to 1MB in size
 * and are divided into chunks. The chunk sizes start off at the size of the
 * "item" structure plus space for a small key and value. They increase by
 * a multiplier factor from there, up to half the maximum slab size. The last
 * slab size is always 1MB, since that's the maximum item size allowed by the
 * memcached protocol.
 */
#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

/* powers-of-N allocation structures */

typedef struct {
    unsigned int size;      /* sizes of items */
    unsigned int perslab;   /* how many items per slab */

    void *slots;           /* 当前可用的空闲item */
    unsigned int sl_curr;   /* total free items in list */

    unsigned int slabs;     /* how many slabs were allocated for this class */

    void **slab_list;       /* array of slab pointers */
    unsigned int list_size; /* size of prev array */

    unsigned int killing;  /* index+1 of dying slab, or zero if none */
    size_t requested; /* The number of requested bytes */
} slabclass_t;

/* 实现内存池管理相关的静态全局变量 */

static slabclass_t slabclass[MAX_NUMBER_OF_SLAB_CLASSES];//定义slab集合，总共200个
static size_t mem_limit = 0;//总的内存大小
static size_t mem_malloced = 0;//初始化内存的大小，这个貌似没什么用
static int power_largest;//已经初始化的最大slab编号
static void *mem_base = NULL;//指向总的内存的首地址
static void *mem_current = NULL;//当前分配到的内存地址
static size_t mem_avail = 0;//当前可用的内存大小

/**
 * Access to the slab allocator is protected by this lock
 */
static pthread_mutex_t slabs_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t slabs_rebalance_lock = PTHREAD_MUTEX_INITIALIZER;

/*
 * Forward Declarations
 */
static int do_slabs_newslab(const unsigned int id);
static void *memory_allocate(size_t size);
static void do_slabs_free(void *ptr, const size_t size, unsigned int id);

/* Preallocate as many slab pages as possible (called from slabs_init)
   on start-up, so users don't get confused out-of-memory errors when
   they do have free (in-slab) space, but no space to make new slabs.
   if maxslabs is 18 (POWER_LARGEST - POWER_SMALLEST + 1), then all
   slab types can be made.  if max memory is less than 18 MB, only the
   smaller ones will be made.  */
static void slabs_preallocate (const unsigned int maxslabs);

/*
 * Figures out which slab class (chunk size) is required to store an item of
 * a given size.
 *
 * Given object size, return id to use when allocating/freeing memory for object
 * 0 means error: can't store such a large object
 */

unsigned int slabs_clsid(const size_t size) {
    int res = POWER_SMALLEST;

    if (size == 0)
        return 0;
    while (size > slabclass[res].size)
        if (res++ == power_largest)     /* won't fit in the biggest slab */
            return 0;
    return res;
}

/**
 * 内存初始化,预分配若干slab的内存空间
 * Determines the chunk sizes and initializes the slab class descriptors
 * accordingly.
 */
void slabs_init(const size_t limit, const double factor, const bool prealloc)
{
    int i = POWER_SMALLEST - 1;
    //size表示申请空间的大小，其值由配置的chunk_size和单个item的大小来指定
    unsigned int size = sizeof(item) + settings.chunk_size;
    mem_limit = limit;//mem_limit是全局变量
    //支持预分配
    if (prealloc)
    {
        /* Allocate everything in a big chunk with malloc */
    	mem_base = malloc(mem_limit);//申请地址，mem_base指向申请的地址
		if (mem_base != NULL)
		{
			mem_current = mem_base;//mem_current指向当前地址
			mem_avail = mem_limit;//可用内存大小为mem_limit
		}
		else
		{//支持预分配失败
            fprintf(stderr, "Warning: Failed to allocate requested memory in"" one large chunk.\nWill allocate in smaller chunks\n");
        }
    }
    //置空slabclass数组
    memset(slabclass, 0, sizeof(slabclass));
    //开始分配，i<200 && 单个chunk的size<单个item最大大小/内存增长因子
    while (++i < POWER_LARGEST && size <= settings.item_size_max / factor)
    {
        /* size 保证 n-byte aligned */
        if (size % CHUNK_ALIGN_BYTES) size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        slabclass[i].size = size;//slab对应chunk的大小
        slabclass[i].perslab = settings.item_size_max / slabclass[i].size;//slab对应的chunk的个数
        size *= factor;//size下一个值为按增长因子的倍数增长
        if (settings.verbose > 1)
        {//如果有打开调试信息，则输出调试信息
            fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n",i, slabclass[i].size, slabclass[i].perslab);
        }
    }
    //循环结束时，size已经增长到1M
    power_largest = i;//再增加一个slab
    slabclass[power_largest].size = settings.item_size_max;//slab的size为item_size_max
    slabclass[power_largest].perslab = 1;//chunk个数为1
    if (settings.verbose > 1)
    {
        fprintf(stderr, "slab class %3d: chunk size %9u perslab %7u\n", i, slabclass[i].size, slabclass[i].perslab);
    }
    /* for the test suite:  faking of how much we've already malloc'd */
    //读取环境变量T_MEMD_INITIAL_MALLOC的值
    {
        char *t_initial_malloc = getenv("T_MEMD_INITIAL_MALLOC");
        if (t_initial_malloc)
        {
            mem_malloced = (size_t)atol(t_initial_malloc);
        }
    }
    if (prealloc)
    {
    	//分配每个slab的内存空间，传入最大已经初始化的最大slab编号
        slabs_preallocate(power_largest);
    }
}

//分配每个slab的内存空间,maxslabs表示已经初始化的slab的最大编号，即下标
static void slabs_preallocate (const unsigned int maxslabs)
{
    int i;
    unsigned int prealloc = 0;
    /* pre-allocate a 1MB slab in every size class so people don't get confused by non-intuitive
    "SERVER_ERROR out of memory" messages.  this is the most common question on the mailing list.
    if you really don't want this, you can rebuild without these three lines.  */
    for (i = POWER_SMALLEST; i <= POWER_LARGEST; i++)
    {
        if (++prealloc > maxslabs) return;
        //执行分配操作，对第i个slabclass执行分配操作
        if (do_slabs_newslab(i) == 0)
        {
            fprintf(stderr, "Error while preallocating slab memory!\n"
                "If using -L or other prealloc options, max memory must be "
                "at least %d megabytes.\n", power_largest);
            exit(1);
        }
    }

}

//初始化slabclass的slab_list,而slab_list中的指针指向每个slab，slab_list保存的只是指针而不是真正的内存,id为slabclass的序号
static int grow_slab_list (const unsigned int id)
{
    slabclass_t *p = &slabclass[id];//p指向第id个slabclass;
    if (p->slabs == p->list_size)
    {
        size_t new_size =  (p->list_size != 0) ? p->list_size * 2 : 16;//new_size如果是首次分配，则取16，否则按旧值的2倍扩容
        void *new_list = realloc(p->slab_list, new_size * sizeof(void *));//申请空间，这个空间是从系统分配，不是从内存池分配
        if (new_list == 0) return 0;
        p->list_size = new_size;//修改第id个slabclass的值
        p->slab_list = new_list;
    }
    return 1;
}

//将ptr指向的内存空间按第id个slabclass的size进行切分
static void split_slab_page_into_freelist(char *ptr, const unsigned int id)
{
    slabclass_t *p = &slabclass[id];
    int x;
    //每个slabclass有多个slab,对每个slab按slabclass对应的size进行切分
    for (x = 0; x < p->perslab; x++)
    {
        do_slabs_free(ptr, 0, id);//创建空闲item
        ptr += p->size;
    }
}

//执行分配操作，id是要分配的slabclass的下标
static int do_slabs_newslab(const unsigned int id)
{
    slabclass_t *p = &slabclass[id];//p指向该slabclass
    int len = settings.slab_reassign ? settings.item_size_max : p->size * p->perslab;
    char *ptr;
    //grow_slab_list初始化slabclass的slab_list，而slab_list中的指针指向每个slab，memory_allocate从内存池申请1M的空间
    if ((mem_limit && mem_malloced + len > mem_limit && p->slabs > 0) ||
        (grow_slab_list(id) == 0) || ((ptr = memory_allocate((size_t)len)) == 0))
    {
        MEMCACHED_SLABS_SLABCLASS_ALLOCATE_FAILED(id);
        return 0;
    }
    memset(ptr, 0, (size_t)len);
    split_slab_page_into_freelist(ptr, id);//将申请的1M空间按slabclass的size进行切分
    p->slab_list[p->slabs++] = ptr;//循环分配
    mem_malloced += len;//增加已经分配出去的内存数
    MEMCACHED_SLABS_SLABCLASS_ALLOCATE(id);
    return 1;
}

/*@null@*/
static void *do_slabs_alloc(const size_t size, unsigned int id) {
    slabclass_t *p;
    void *ret = NULL;
    item *it = NULL;

    if (id < POWER_SMALLEST || id > power_largest) {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, 0);
        return NULL;
    }

    p = &slabclass[id];
    assert(p->sl_curr == 0 || ((item *)p->slots)->slabs_clsid == 0);

    /* fail unless we have space at the end of a recently allocated page,
       we have something on our freelist, or we could allocate a new page */
    /* 确保最后一个slab 有空闲的chunk */
    if (! (p->sl_curr != 0 || do_slabs_newslab(id) != 0))
    {
        /* We don't have more memory available */
        ret = NULL;
    }
    //从slabclass_t的空闲item链表中获取一个item
    else if (p->sl_curr != 0)
    {
        /* return off our freelist */
        it = (item *)p->slots;
        p->slots = it->next;
        if (it->next) it->next->prev = 0;
        p->sl_curr--;
        ret = (void *)it;
    }

    if (ret)
    {
        p->requested += size;
        MEMCACHED_SLABS_ALLOCATE(size, id, p->size, ret);
    }
    else {
        MEMCACHED_SLABS_ALLOCATE_FAILED(size, id);
    }

    return ret;
}

//创建或回收空闲的item
static void do_slabs_free(void *ptr, const size_t size, unsigned int id)
{
    slabclass_t *p;
    item *it;

    assert(((item *)ptr)->slabs_clsid == 0);//判断数据是否正确
	assert(id >= POWER_SMALLEST && id <= power_largest);//判断id合法性
	if (id < POWER_SMALLEST || id > power_largest)//判断id合法性
		return;

    MEMCACHED_SLABS_FREE(size, id, ptr);
    p = &slabclass[id];

    it = (item *)ptr;
    it->it_flags |= ITEM_SLABBED;//修改item的状态标识，修改为空闲

    /* 采用倒插法插入subclass_t->slots中 */
    it->prev = 0;//断开数据链表
    it->next = p->slots;//挂载到slabclass的空闲链表中
    if (it->next) it->next->prev = it;
    p->slots = it;

    p->sl_curr++;//空闲item个数+1
    p->requested -= size;//已经申请到的空间数量更新
    return;
}

static int nz_strcmp(int nzlength, const char *nz, const char *z) {
    int zlength=strlen(z);
    return (zlength == nzlength) && (strncmp(nz, z, zlength) == 0) ? 0 : -1;
}

bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c) {
    bool ret = true;

    if (add_stats != NULL) {
        if (!stat_type) {
            /* prepare general statistics for the engine */
            STATS_LOCK();
            APPEND_STAT("bytes", "%llu", (unsigned long long)stats.curr_bytes);
            APPEND_STAT("curr_items", "%u", stats.curr_items);
            APPEND_STAT("total_items", "%u", stats.total_items);
            STATS_UNLOCK();
            item_stats_totals(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "items") == 0) {
            item_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "slabs") == 0) {
            slabs_stats(add_stats, c);
        } else if (nz_strcmp(nkey, stat_type, "sizes") == 0) {
            item_stats_sizes(add_stats, c);
        } else {
            ret = false;
        }
    } else {
        ret = false;
    }

    return ret;
}

/*@null@*/
static void do_slabs_stats(ADD_STAT add_stats, void *c) {
    int i, total;
    /* Get the per-thread stats which contain some interesting aggregates */
    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);

    total = 0;
    for(i = POWER_SMALLEST; i <= power_largest; i++) {
        slabclass_t *p = &slabclass[i];
        if (p->slabs != 0) {
            uint32_t perslab, slabs;
            slabs = p->slabs;
            perslab = p->perslab;

            char key_str[STAT_KEY_LEN];
            char val_str[STAT_VAL_LEN];
            int klen = 0, vlen = 0;

            APPEND_NUM_STAT(i, "chunk_size", "%u", p->size);
            APPEND_NUM_STAT(i, "chunks_per_page", "%u", perslab);
            APPEND_NUM_STAT(i, "total_pages", "%u", slabs);
            APPEND_NUM_STAT(i, "total_chunks", "%u", slabs * perslab);
            APPEND_NUM_STAT(i, "used_chunks", "%u",
                            slabs*perslab - p->sl_curr);
            APPEND_NUM_STAT(i, "free_chunks", "%u", p->sl_curr);
            /* Stat is dead, but displaying zero instead of removing it. */
            APPEND_NUM_STAT(i, "free_chunks_end", "%u", 0);
            APPEND_NUM_STAT(i, "mem_requested", "%llu",
                            (unsigned long long)p->requested);
            APPEND_NUM_STAT(i, "get_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].get_hits);
            APPEND_NUM_STAT(i, "cmd_set", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].set_cmds);
            APPEND_NUM_STAT(i, "delete_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].delete_hits);
            APPEND_NUM_STAT(i, "incr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].incr_hits);
            APPEND_NUM_STAT(i, "decr_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].decr_hits);
            APPEND_NUM_STAT(i, "cas_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_hits);
            APPEND_NUM_STAT(i, "cas_badval", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].cas_badval);
            APPEND_NUM_STAT(i, "touch_hits", "%llu",
                    (unsigned long long)thread_stats.slab_stats[i].touch_hits);
            total++;
        }
    }

    /* add overall slab stats and append terminator */

    APPEND_STAT("active_slabs", "%d", total);
    APPEND_STAT("total_malloced", "%llu", (unsigned long long)mem_malloced);
    add_stats(NULL, 0, NULL, 0, c);
}

//从内存池分配size个空间
static void *memory_allocate(size_t size)
{
    void *ret;

    //如果内存池没创建，则从系统分配
    if (mem_base == NULL)
    {
        /* We are not using a preallocated large memory chunk */
        ret = malloc(size);
    }
    else
    {
        ret = mem_current;

        //size大于剩余的空间
        if (size > mem_avail)
        {
            return NULL;
        }

        /* mem_current pointer _must_ be aligned!!! */
        if (size % CHUNK_ALIGN_BYTES)
        {
            size += CHUNK_ALIGN_BYTES - (size % CHUNK_ALIGN_BYTES);
        }
        //扣除size个空间
        mem_current = ((char*)mem_current) + size;
        if (size < mem_avail)
        {
            mem_avail -= size;//更新剩余空间大小
        }
        else
        {
            mem_avail = 0;
        }
    }

    return ret;
}

void *slabs_alloc(size_t size, unsigned int id) {
    void *ret;

    pthread_mutex_lock(&slabs_lock);
    ret = do_slabs_alloc(size, id);
    pthread_mutex_unlock(&slabs_lock);
    return ret;
}

//slabclass结构释放
void slabs_free(void *ptr, size_t size, unsigned int id) {
    pthread_mutex_lock(&slabs_lock);//保持同步
    do_slabs_free(ptr, size, id);//执行释放
    pthread_mutex_unlock(&slabs_lock);
}

void slabs_stats(ADD_STAT add_stats, void *c) {
    pthread_mutex_lock(&slabs_lock);
    do_slabs_stats(add_stats, c);
    pthread_mutex_unlock(&slabs_lock);
}

void slabs_adjust_mem_requested(unsigned int id, size_t old, size_t ntotal)
{
    pthread_mutex_lock(&slabs_lock);
    slabclass_t *p;
    if (id < POWER_SMALLEST || id > power_largest) {
        fprintf(stderr, "Internal error! Invalid slab class\n");
        abort();
    }

    p = &slabclass[id];
    p->requested = p->requested - old + ntotal;
    pthread_mutex_unlock(&slabs_lock);
}

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_cond_t slab_rebalance_cond = PTHREAD_COND_INITIALIZER;
static volatile int do_run_slab_thread = 1;
static volatile int do_run_slab_rebalance_thread = 1;

#define DEFAULT_SLAB_BULK_CHECK 1
int slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;

static int slab_rebalance_start(void) {
    slabclass_t *s_cls;
    int no_go = 0;

    pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&slabs_lock);

    if (slab_rebal.s_clsid < POWER_SMALLEST ||
        slab_rebal.s_clsid > power_largest  ||
        slab_rebal.d_clsid < POWER_SMALLEST ||
        slab_rebal.d_clsid > power_largest  ||
        slab_rebal.s_clsid == slab_rebal.d_clsid)
        no_go = -2;

    s_cls = &slabclass[slab_rebal.s_clsid];

    if (!grow_slab_list(slab_rebal.d_clsid)) {
        no_go = -1;
    }

    if (s_cls->slabs < 2)
        no_go = -3;

    if (no_go != 0) {
        pthread_mutex_unlock(&slabs_lock);
        pthread_mutex_unlock(&cache_lock);
        return no_go; /* Should use a wrapper function... */
    }

    s_cls->killing = 1;

    slab_rebal.slab_start = s_cls->slab_list[s_cls->killing - 1];
    slab_rebal.slab_end   = (char *)slab_rebal.slab_start +
        (s_cls->size * s_cls->perslab);
    slab_rebal.slab_pos   = slab_rebal.slab_start;
    slab_rebal.done       = 0;

    /* Also tells do_item_get to search for items in this slab */
    slab_rebalance_signal = 2;

    if (settings.verbose > 1) {
        fprintf(stderr, "Started a slab rebalance\n");
    }

    pthread_mutex_unlock(&slabs_lock);
    pthread_mutex_unlock(&cache_lock);

    STATS_LOCK();
    stats.slab_reassign_running = true;
    STATS_UNLOCK();

    return 0;
}

enum move_status {
    MOVE_PASS=0, MOVE_DONE, MOVE_BUSY, MOVE_LOCKED
};

/* refcount == 0 is safe since nobody can incr while cache_lock is held.
 * refcount != 0 is impossible since flags/etc can be modified in other
 * threads. instead, note we found a busy one and bail. logic in do_item_get
 * will prevent busy items from continuing to be busy
 */
static int slab_rebalance_move(void) {
    slabclass_t *s_cls;
    int x;
    int was_busy = 0;
    int refcount = 0;
    enum move_status status = MOVE_PASS;

    pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&slabs_lock);

    s_cls = &slabclass[slab_rebal.s_clsid];

    for (x = 0; x < slab_bulk_check; x++) {
        item *it = slab_rebal.slab_pos;
        status = MOVE_PASS;
        if (it->slabs_clsid != 255) {
            void *hold_lock = NULL;
            uint32_t hv = hash(ITEM_key(it), it->nkey, 0);
            if ((hold_lock = item_trylock(hv)) == NULL) {
                status = MOVE_LOCKED;
            } else {
                refcount = refcount_incr(&it->refcount);
                if (refcount == 1) { /* item is unlinked, unused */
                    if (it->it_flags & ITEM_SLABBED) {
                        /* remove from slab freelist */
                        if (s_cls->slots == it) {
                            s_cls->slots = it->next;
                        }
                        if (it->next) it->next->prev = it->prev;
                        if (it->prev) it->prev->next = it->next;
                        s_cls->sl_curr--;
                        status = MOVE_DONE;
                    } else {
                        status = MOVE_BUSY;
                    }
                } else if (refcount == 2) { /* item is linked but not busy */
                    if ((it->it_flags & ITEM_LINKED) != 0) {
                        do_item_unlink_nolock(it, hash(ITEM_key(it), it->nkey, 0));
                        status = MOVE_DONE;
                    } else {
                        /* refcount == 1 + !ITEM_LINKED means the item is being
                         * uploaded to, or was just unlinked but hasn't been freed
                         * yet. Let it bleed off on its own and try again later */
                        status = MOVE_BUSY;
                    }
                } else {
                    if (settings.verbose > 2) {
                        fprintf(stderr, "Slab reassign hit a busy item: refcount: %d (%d -> %d)\n",
                            it->refcount, slab_rebal.s_clsid, slab_rebal.d_clsid);
                    }
                    status = MOVE_BUSY;
                }
                item_trylock_unlock(hold_lock);
            }
        }

        switch (status) {
            case MOVE_DONE:
                it->refcount = 0;
                it->it_flags = 0;
                it->slabs_clsid = 255;
                break;
            case MOVE_BUSY:
                refcount_decr(&it->refcount);
            case MOVE_LOCKED:
                slab_rebal.busy_items++;
                was_busy++;
                break;
            case MOVE_PASS:
                break;
        }

        slab_rebal.slab_pos = (char *)slab_rebal.slab_pos + s_cls->size;
        if (slab_rebal.slab_pos >= slab_rebal.slab_end)
            break;
    }

    if (slab_rebal.slab_pos >= slab_rebal.slab_end) {
        /* Some items were busy, start again from the top */
        if (slab_rebal.busy_items) {
            slab_rebal.slab_pos = slab_rebal.slab_start;
            slab_rebal.busy_items = 0;
        } else {
            slab_rebal.done++;
        }
    }

    pthread_mutex_unlock(&slabs_lock);
    pthread_mutex_unlock(&cache_lock);

    return was_busy;
}

static void slab_rebalance_finish(void) {
    slabclass_t *s_cls;
    slabclass_t *d_cls;

    pthread_mutex_lock(&cache_lock);
    pthread_mutex_lock(&slabs_lock);

    s_cls = &slabclass[slab_rebal.s_clsid];
    d_cls   = &slabclass[slab_rebal.d_clsid];

    /* At this point the stolen slab is completely clear */
    s_cls->slab_list[s_cls->killing - 1] =
        s_cls->slab_list[s_cls->slabs - 1];
    s_cls->slabs--;
    s_cls->killing = 0;

    memset(slab_rebal.slab_start, 0, (size_t)settings.item_size_max);

    d_cls->slab_list[d_cls->slabs++] = slab_rebal.slab_start;
    split_slab_page_into_freelist(slab_rebal.slab_start,
        slab_rebal.d_clsid);

    slab_rebal.done       = 0;
    slab_rebal.s_clsid    = 0;
    slab_rebal.d_clsid    = 0;
    slab_rebal.slab_start = NULL;
    slab_rebal.slab_end   = NULL;
    slab_rebal.slab_pos   = NULL;

    slab_rebalance_signal = 0;

    pthread_mutex_unlock(&slabs_lock);
    pthread_mutex_unlock(&cache_lock);

    STATS_LOCK();
    stats.slab_reassign_running = false;
    stats.slabs_moved++;
    STATS_UNLOCK();

    if (settings.verbose > 1) {
        fprintf(stderr, "finished a slab move\n");
    }
}

/* Return 1 means a decision was reached.
 * Move to its own thread (created/destroyed as needed) once automover is more complex.
 * 选择src和dest的处理函数，src是失败次数没有，或者很平均的那个slab-class id，dest是那些分配失败次数骤增的slab-class id
 */
static int slab_automove_decision(int *src, int *dst)
{
    static uint64_t evicted_old[POWER_LARGEST];//static的成员变量，记录上次的分配失败统计信息，一个slab-class一个元素
    static unsigned int slab_zeroes[POWER_LARGEST];//记录每个slab-class,分配失败次数不变化的次数
    static unsigned int slab_winner = 0;// 表示哪个slab-class应该是那个dst
    static unsigned int slab_wins   = 0;//选举成功的次数
    uint64_t evicted_new[POWER_LARGEST];//新的slab分配失败统计信息
    uint64_t evicted_diff = 0;//记录每个slab新的统计信息和旧的统计信息的差
    uint64_t evicted_max  = 0;//记录分配失败次数的最大值
    unsigned int highest_slab = 0;//记录分配次数最多的那个slab-class id
    unsigned int total_pages[POWER_LARGEST];
    int i;
    int source = 0;
    int dest = 0;
    static rel_time_t next_run;
    /* Run less frequently than the slabmove tester. */
    if (current_time >= next_run)
    {
        next_run = current_time + 10;
    }
    else
    {
        return 0;
    }
    item_stats_evictions(evicted_new);
    pthread_mutex_lock(&cache_lock);
    for (i = POWER_SMALLEST; i < power_largest; i++) {
        total_pages[i] = slabclass[i].slabs;
    }
    pthread_mutex_unlock(&cache_lock);

    /* Find a candidate source; something with zero evicts 3+ times */
    for (i = POWER_SMALLEST; i < power_largest; i++)
    {
        evicted_diff = evicted_new[i] - evicted_old[i];
        if (evicted_diff == 0 && total_pages[i] > 2)
        {
            slab_zeroes[i]++;
            if (source == 0 && slab_zeroes[i] >= 3)
                source = i;
        }
        else
        {
            slab_zeroes[i] = 0;
            if (evicted_diff > evicted_max)
            {
                evicted_max = evicted_diff;
                highest_slab = i;
            }
        }
        evicted_old[i] = evicted_new[i];
    }
    /* Pick a valid destination */
    if (slab_winner != 0 && slab_winner == highest_slab)
    {
        slab_wins++;
        if (slab_wins >= 3)
            dest = slab_winner;
    }
    else
    {
        slab_wins = 1;
        slab_winner = highest_slab;
    }
    if (source && dest)
    {
        *src = source;
        *dst = dest;
        return 1;
    }
    return 0;
}

/* Slab rebalancer thread.
 * Does not use spinlocks since it is not timing sensitive. Burn less CPU and
 * go to sleep if locks are contended
 */
static void *slab_maintenance_thread(void *arg)
{
    int src, dest; //src是失败次数很少，或者很平均的那个slab-class id，dest是那些分配失败次数骤增的slab-class id
    while (do_run_slab_thread)
    {
        if (settings.slab_automove == 1)//如果是前面的激烈模式,slab_automove=2,就不用该线程去选择移动的slab，每次失败主动会去移动那些slab
        {
            if (slab_automove_decision(&src, &dest) == 1)//这里是具体的选择src和dest的处理函数
            {
                /* Blind to the return codes. It will retry on its own */
                slabs_reassign(src, dest);
            }
            sleep(1);
        }
        else
        {
            /* Don't wake as often if we're not enabled.
             * This is lazier than setting up a condition right now. */
            sleep(5);
        }
    }
    return NULL;
}

/* Slab mover thread.
 * Sits waiting for a condition to jump off and shovel some memory about
 */
static void *slab_rebalance_thread(void *arg)
{
    int was_busy = 0;
    /* So we first pass into cond_wait with the mutex held */
    mutex_lock(&slabs_rebalance_lock);
    while (do_run_slab_rebalance_thread)
    {
        if (slab_rebalance_signal == 1)
        {
            if (slab_rebalance_start() < 0) //确定src中删除开始的slab开始的指针。
            {
                /* Handle errors with more specifity as required. */
                slab_rebalance_signal = 0;
            }
            was_busy = 0;
        }
        else if (slab_rebalance_signal && slab_rebal.slab_start != NULL)
        {
        	 /* 开始删除src的那个slab item（hash结构，lru链表），每次删除1个item，函数在while结构中会被重复执行，直到那个slab中item被完全
        	 删除成功，每次删除一个也是为了控制锁的持有时间，毕竟这个锁持有时间越长，就会导致hash结构插入操作等待锁的时间变长 */
            was_busy = slab_rebalance_move();
        }
        if (slab_rebal.done) //如果已经删除完毕，开始把slab_rebal中的空间给dest，这个空间来源于上部删除的src的空间
        {
            slab_rebalance_finish();
        }
        else if (was_busy)
        {
            /* Stuck waiting for some items to unlock, so slow down a bit
             * to give them a chance to free up */
            usleep(50);
        }
        if (slab_rebalance_signal == 0)
        {
            /* always hold this lock while we're running */
            pthread_cond_wait(&slab_rebalance_cond, &slabs_rebalance_lock);
        }
    }
    return NULL;
}

/* Iterate at most once through the slab classes and pick a "random" source.
 * I like this better than calling rand() since rand() is slow enough that we
 * can just check all of the classes once instead.
 */
static int slabs_reassign_pick_any(int dst) {
    static int cur = POWER_SMALLEST - 1;
    int tries = power_largest - POWER_SMALLEST + 1;
    for (; tries > 0; tries--) {
        cur++;
        if (cur > power_largest)
            cur = POWER_SMALLEST;
        if (cur == dst)
            continue;
        if (slabclass[cur].slabs > 1) {
            return cur;
        }
    }
    return -1;
}

static enum reassign_result_type do_slabs_reassign(int src, int dst) {
    if (slab_rebalance_signal != 0)
        return REASSIGN_RUNNING;

    if (src == dst)
        return REASSIGN_SRC_DST_SAME;

    /* Special indicator to choose ourselves. */
    if (src == -1) {
        src = slabs_reassign_pick_any(dst);
        /* TODO: If we end up back at -1, return a new error type */
    }

    if (src < POWER_SMALLEST || src > power_largest ||
        dst < POWER_SMALLEST || dst > power_largest)
        return REASSIGN_BADCLASS;

    if (slabclass[src].slabs < 2)
        return REASSIGN_NOSPARE;

    slab_rebal.s_clsid = src;
    slab_rebal.d_clsid = dst;

    slab_rebalance_signal = 1;
    pthread_cond_signal(&slab_rebalance_cond);

    return REASSIGN_OK;
}

enum reassign_result_type slabs_reassign(int src, int dst) {
    enum reassign_result_type ret;
    if (pthread_mutex_trylock(&slabs_rebalance_lock) != 0) {
        return REASSIGN_RUNNING;
    }
    ret = do_slabs_reassign(src, dst);
    pthread_mutex_unlock(&slabs_rebalance_lock);
    return ret;
}

/* If we hold this lock, rebalancer can't wake up or move */
void slabs_rebalancer_pause(void) {
    pthread_mutex_lock(&slabs_rebalance_lock);
}

void slabs_rebalancer_resume(void) {
    pthread_mutex_unlock(&slabs_rebalance_lock);
}

static pthread_t maintenance_tid;
static pthread_t rebalance_tid;

//创建两个slab维护线程，具体参考http://blog.chinaunix.net/uid-27767798-id-3404133.html
int start_slab_maintenance_thread(void)
{
    int ret;
    slab_rebalance_signal = 0;
    slab_rebal.slab_start = NULL;
    char *env = getenv("MEMCACHED_SLAB_BULK_CHECK");
    if (env != NULL)
    {
        slab_bulk_check = atoi(env);
        if (slab_bulk_check == 0) {
            slab_bulk_check = DEFAULT_SLAB_BULK_CHECK;
        }
    }
    if (pthread_cond_init(&slab_rebalance_cond, NULL) != 0)
    {
        fprintf(stderr, "Can't intiialize rebalance condition\n");
        return -1;
    }
    pthread_mutex_init(&slabs_rebalance_lock, NULL);
    //slab线程，维护slab空间的大小
    if ((ret = pthread_create(&maintenance_tid, NULL,slab_maintenance_thread, NULL)) != 0)
    {
        fprintf(stderr, "Can't create slab maint thread: %s\n", strerror(ret));
        return -1;
    }
    //slab线程，维护slab的平衡
    if ((ret = pthread_create(&rebalance_tid, NULL,slab_rebalance_thread, NULL)) != 0)
    {
        fprintf(stderr, "Can't create rebal thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_slab_maintenance_thread(void) {
    mutex_lock(&cache_lock);
    do_run_slab_thread = 0;
    do_run_slab_rebalance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    pthread_mutex_unlock(&cache_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
    pthread_join(rebalance_tid, NULL);
}