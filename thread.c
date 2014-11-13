#include "memcached.h"
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>

#ifdef __sun
#include <atomic.h>
#endif

#define ITEMS_PER_ALLOC 64

/* An item in the connection queue. */
typedef struct conn_queue_item CQ_ITEM;
struct conn_queue_item {
    int               sfd;
    enum conn_states  init_state;
    int               event_flags;
    int               read_buffer_size;
    enum network_transport     transport;
    CQ_ITEM          *next;
};

/* A connection queue. */
typedef struct conn_queue CQ;
struct conn_queue {
    CQ_ITEM *head;
    CQ_ITEM *tail;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
};

/* Lock for cache operations (item_*, assoc_*) */
pthread_mutex_t cache_lock;

/* Connection lock around accepting new connections */
pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

#if !defined(HAVE_GCC_ATOMICS) && !defined(__sun)
pthread_mutex_t atomics_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif

/* Lock for global stats */
static pthread_mutex_t stats_lock;

/* Free list of CQ_ITEM structs */
static CQ_ITEM *cqi_freelist;
static pthread_mutex_t cqi_freelist_lock;

static pthread_mutex_t *item_locks;
/* size of the item lock hash table */
static uint32_t item_lock_count;
#define hashsize(n) ((unsigned long int)1<<(n))
#define hashmask(n) (hashsize(n)-1)
/* this lock is temporarily engaged during a hash table expansion */
static pthread_mutex_t item_global_lock;
/* thread-specific variable for deeply finding the item lock type */
static pthread_key_t item_lock_type_key;

static LIBEVENT_DISPATCHER_THREAD dispatcher_thread;

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
static LIBEVENT_THREAD *threads;

/*
 * Number of worker threads that have finished setting themselves up.
 */
static int init_count = 0;
static pthread_mutex_t init_lock;
static pthread_cond_t init_cond;


static void thread_libevent_process(int fd, short which, void *arg);

unsigned short refcount_incr(unsigned short *refcount) {
#ifdef HAVE_GCC_ATOMICS
    return __sync_add_and_fetch(refcount, 1);
#elif defined(__sun)
    return atomic_inc_ushort_nv(refcount);
#else
    unsigned short res;
    mutex_lock(&atomics_mutex);
    (*refcount)++;
    res = *refcount;
    mutex_unlock(&atomics_mutex);
    return res;
#endif
}

unsigned short refcount_decr(unsigned short *refcount) {
#ifdef HAVE_GCC_ATOMICS
    return __sync_sub_and_fetch(refcount, 1);
#elif defined(__sun)
    return atomic_dec_ushort_nv(refcount);
#else
    unsigned short res;
    mutex_lock(&atomics_mutex);
    (*refcount)--;
    res = *refcount;
    mutex_unlock(&atomics_mutex);
    return res;
#endif
}

/* Convenience functions for calling *only* when in ITEM_LOCK_GLOBAL mode */
void item_lock_global(void) {
    mutex_lock(&item_global_lock);
}

void item_unlock_global(void) {
    mutex_unlock(&item_global_lock);
}

//执行分段加锁
void item_lock(uint32_t hv)
{
    uint8_t *lock_type = pthread_getspecific(item_lock_type_key);
    if (likely(*lock_type == ITEM_LOCK_GRANULAR))
    {
        mutex_lock(&item_locks[(hv & hashmask(hashpower)) % item_lock_count]);//执行分段加锁
    }
    else
    {//如果在扩容过程中
        mutex_lock(&item_global_lock);
    }
}

/* Special case. When ITEM_LOCK_GLOBAL mode is enabled, this should become a
 * no-op, as it's only called from within the item lock if necessary.
 * However, we can't mix a no-op and threads which are still synchronizing to
 * GLOBAL. So instead we just always try to lock. When in GLOBAL mode this
 * turns into an effective no-op. Threads re-synchronize after the power level
 * switch so it should stay safe.
 */
void *item_trylock(uint32_t hv) {
    pthread_mutex_t *lock = &item_locks[(hv & hashmask(hashpower)) % item_lock_count];
    if (pthread_mutex_trylock(lock) == 0) {
        return lock;
    }
    return NULL;
}

void item_trylock_unlock(void *lock) {
    mutex_unlock((pthread_mutex_t *) lock);
}

//执行分段解锁
void item_unlock(uint32_t hv)
{
    uint8_t *lock_type = pthread_getspecific(item_lock_type_key);
    if (likely(*lock_type == ITEM_LOCK_GRANULAR))
    {
        mutex_unlock(&item_locks[(hv & hashmask(hashpower)) % item_lock_count]);//释放分段锁
    }
    else
    {//如果在扩容过程中
        mutex_unlock(&item_global_lock);
    }
}

//阻塞工作线程
static void wait_for_thread_registration(int nthreads)
{
    while (init_count < nthreads)
    {
        pthread_cond_wait(&init_cond, &init_lock);//在条件变量init_cond上面阻塞，阻塞个数为nthreads-init_count
    }
}

//唤醒工作线程
static void register_thread_initialized(void)
{
    pthread_mutex_lock(&init_lock);
    init_count++;//计算已经初始化完成的线程数
    pthread_cond_signal(&init_cond);//http://baike.baidu.com/view/7844657.htm?fr=aladdin
    pthread_mutex_unlock(&init_lock);
}

void switch_item_lock_type(enum item_lock_types type) {
    char buf[1];
    int i;

    switch (type) {
        case ITEM_LOCK_GRANULAR:
            buf[0] = 'l';
            break;
        case ITEM_LOCK_GLOBAL:
            buf[0] = 'g';
            break;
        default:
            fprintf(stderr, "Unknown lock type: %d\n", type);
            assert(1 == 0);
            break;
    }

    pthread_mutex_lock(&init_lock);
    init_count = 0;
    for (i = 0; i < settings.num_threads; i++) {
        if (write(threads[i].notify_send_fd, buf, 1) != 1) {
            perror("Failed writing to notify pipe");
            /* TODO: This is a fatal problem. Can it ever happen temporarily? */
        }
    }
    wait_for_thread_registration(settings.num_threads);
    pthread_mutex_unlock(&init_lock);
}

/*
 * Initializes a connection queue.
 */
static void cq_init(CQ *cq) {
    pthread_mutex_init(&cq->lock, NULL);
    pthread_cond_init(&cq->cond, NULL);
    cq->head = NULL;
    cq->tail = NULL;
}

/*
 * Looks for an item on a connection queue, but doesn't block if there isn't
 * one.
 * Returns the item, or NULL if no item is available
 */
static CQ_ITEM *cq_pop(CQ *cq) {
    CQ_ITEM *item;

    pthread_mutex_lock(&cq->lock);
    item = cq->head;
    if (NULL != item) {
        cq->head = item->next;
        if (NULL == cq->head)
            cq->tail = NULL;
    }
    pthread_mutex_unlock(&cq->lock);

    return item;
}

/*
 * Adds an item to a connection queue.
 */
static void cq_push(CQ *cq, CQ_ITEM *item) {
    item->next = NULL;

    pthread_mutex_lock(&cq->lock);
    if (NULL == cq->tail)
        cq->head = item;
    else
        cq->tail->next = item;
    cq->tail = item;
    pthread_cond_signal(&cq->cond);
    pthread_mutex_unlock(&cq->lock);
}

/*
 * Returns a fresh connection queue item.
 */
static CQ_ITEM *cqi_new(void) {
    CQ_ITEM *item = NULL;
    pthread_mutex_lock(&cqi_freelist_lock);
    if (cqi_freelist) {
        item = cqi_freelist;
        cqi_freelist = item->next;
    }
    pthread_mutex_unlock(&cqi_freelist_lock);

    if (NULL == item) {
        int i;

        /* Allocate a bunch of items at once to reduce fragmentation */
        item = malloc(sizeof(CQ_ITEM) * ITEMS_PER_ALLOC);
        if (NULL == item)
            return NULL;

        /*
         * Link together all the new items except the first one
         * (which we'll return to the caller) for placement on
         * the freelist.
         */
        for (i = 2; i < ITEMS_PER_ALLOC; i++)
            item[i - 1].next = &item[i];

        pthread_mutex_lock(&cqi_freelist_lock);
        item[ITEMS_PER_ALLOC - 1].next = cqi_freelist;
        cqi_freelist = &item[1];
        pthread_mutex_unlock(&cqi_freelist_lock);
    }

    return item;
}


/*
 * Frees a connection queue item (adds it to the freelist.)
 */
static void cqi_free(CQ_ITEM *item) {
    pthread_mutex_lock(&cqi_freelist_lock);
    item->next = cqi_freelist;
    cqi_freelist = item;
    pthread_mutex_unlock(&cqi_freelist_lock);
}


/*  创建工作线程，线程处理函数fun为worker_libevent，参数arg为LIBEVENT_THREAD* */
static void create_worker(void *(*func)(void *), void *arg)
{
    pthread_t       thread;
    pthread_attr_t  attr;
    int             ret;
    pthread_attr_init(&attr);//Posix线程部分，线程属性初始化
    //通过pthread_create创建线程，线程处理函数是通过外部传入的处理函数为worker_libevent
    if ((ret = pthread_create(&thread, &attr, func, arg)) != 0)
    {
        fprintf(stderr, "Can't create thread: %s\n",strerror(ret));
        exit(1);
    }
}

/*
 * Sets whether or not we accept new connections.
 */
void accept_new_conns(const bool do_accept) {
    pthread_mutex_lock(&conn_lock);
    do_accept_new_conns(do_accept);
    pthread_mutex_unlock(&conn_lock);
}
/****************************** LIBEVENT THREADS *****************************/

/* 工作线程绑定到libevent实例 */
static void setup_thread(LIBEVENT_THREAD *me)
{
	me->base = event_init();//创建libevent实例
	if (! me->base)
	{
		fprintf(stderr, "Can't allocate event base\n");
		exit(1);
	}
	//创建管道读的libevent事件，事件的回调函数处理具体的业务信息，关于回调函数的处理，后续分析
	event_set(&me->notify_event, me->notify_receive_fd, EV_READ | EV_PERSIST, thread_libevent_process, me);
	event_base_set(me->base, &me->notify_event);//设置libevent实例
	//添加事件到libevent中
	if (event_add(&me->notify_event, 0) == -1)
	{
		fprintf(stderr, "Can't monitor libevent notify pipe\n");
		exit(1);
	}
	//创建消息队列，用于接受主线程连接
	me->new_conn_queue = malloc(sizeof(struct conn_queue));
	if (me->new_conn_queue == NULL)
	{
		perror("Failed to allocate memory for connection queue");
		exit(EXIT_FAILURE);
	}
	cq_init(me->new_conn_queue);//消息队列初始化
	if (pthread_mutex_init(&me->stats.mutex, NULL) != 0)
	{
		perror("Failed to initialize mutex");
		exit(EXIT_FAILURE);
	}
	//创建线程的后缀cache,没搞懂这个cache有什么作用。
	me->suffix_cache = cache_create("suffix", SUFFIX_SIZE, sizeof(char*),NULL, NULL);
	if (me->suffix_cache == NULL)
	{
		fprintf(stderr, "Failed to create suffix cache\n");
		exit(EXIT_FAILURE);
	}
}

/* Worker thread 的libevent事件循环函数，参数arg为LIBEVENT_THREAD* */
static void *worker_libevent(void *arg)
{
    LIBEVENT_THREAD *me = arg;
    /* Any per-thread setup can happen here; thread_init() will block until
     * all threads have finished initializing.
     */

    /* set an indexable thread-specific memory item for the lock type.
     * this could be unnecessary if we pass the conn *c struct through
     * all item_lock calls...
     */
    me->item_lock_type = ITEM_LOCK_GRANULAR;//默认的hash表的锁为局部锁
    pthread_setspecific(item_lock_type_key, &me->item_lock_type);//设定线程的私有数据
    register_thread_initialized(); //用于控制工作线程初始化，通过条件变量来控制
    event_base_loop(me->base, 0);//进入libevent的事件循环等待函数，等待事件被触发，然后调用它们的回调函数thread_libevent_process
    return NULL;
}

/* Processes an incoming "handle a new connection" item. This is called when input arrives on the libevent wakeup pipe.
 * 当有读事件到达时，即主线程发送一个c字符到该线程的管道后，worker线程调用该函数，该线程进入drive_machine处理机进行事件处理*/
static void thread_libevent_process(int fd, short which, void *arg)
{
    LIBEVENT_THREAD *me = arg;
    CQ_ITEM *item;
    char buf[1];
    if (read(fd, buf, 1) != 1)//PIPE管道读取一个字节的数据
        if (settings.verbose > 0) fprintf(stderr, "Can't read from libevent pipe\n");
    switch (buf[0])
    {
    case 'c'://如果是c，则处理网络连接
    item = cq_pop(me->new_conn_queue);//从连接队列读出Master线程投递的消息
    if (NULL != item)
    {
    	//创建连接，并进入libevent的事件处理函数event_handler
        conn *c = conn_new(item->sfd, item->init_state, item->event_flags, item->read_buffer_size, item->transport, me->base);
        if (c == NULL)
        {
            if (IS_UDP(item->transport))
            {
                fprintf(stderr, "Can't listen for events on UDP socket\n");
                exit(1);
            }
            else
            {
                if (settings.verbose > 0) {
                    fprintf(stderr, "Can't listen for events on fd %d\n",item->sfd);
                }
                close(item->sfd);
            }
        }
        else
        {
            c->thread = me;
        }
        cqi_free(item);
    }
        break;
    }
}
/* Which thread we assigned a connection to most recently. */
static int last_thread = -1;

/* Dispatches a new connection to another thread. This is only ever called from the main thread, either during
 * initialization (for UDP) or because of an incoming connection.连接状态init_state 为 conn_new_cmd，evnet_flags
 * 为EV_READ | EV_PERSIST */
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags,
						int read_buffer_size, enum network_transport transport)
{
    CQ_ITEM *item = cqi_new();//创建一个连接队列
    char buf[1];
    int tid = (last_thread + 1) % settings.num_threads;//通过round-robin算法选择一个线程
    LIBEVENT_THREAD *thread = threads + tid;//thread数组存储了所有的工作线程
    last_thread = tid;//缓存这次的线程编号，下次待用
    item->sfd = sfd;//sfd表示accept之后的描述符
    item->init_state = init_state;
    item->event_flags = event_flags;
    item->read_buffer_size = read_buffer_size;
    item->transport = transport;
    cq_push(thread->new_conn_queue, item);//投递item信息到Worker线程的工作队列中
    MEMCACHED_CONN_DISPATCH(sfd, thread->thread_id);
    buf[0] = 'c';
    //在Worker线程的notify_send_fd写入字符c，表示有连接，触发thread_libevent_process函数
    if (write(thread->notify_send_fd, buf, 1) != 1)
    {
        perror("Writing to thread notify pipe");
    }
}

/* Returns true if this is the thread that listens for new TCP connections. */
int is_listen_thread() {
    return pthread_self() == dispatcher_thread.thread_id;
}

/********************************* ITEM ACCESS *******************************/

/*
 * Allocates a new item.
 */
item *item_alloc(char *key, size_t nkey, int flags, rel_time_t exptime, int nbytes) {
    item *it;
    /* do_item_alloc handles its own locks */
    it = do_item_alloc(key, nkey, flags, exptime, nbytes, 0);
    return it;
}

/*
 * Returns an item if it hasn't been marked as expired, lazy-expiring as needed.根据key信息和key的长度信息读取数据
 */
item *item_get(const char *key, const size_t nkey)
{
    item *it;
    uint32_t hv;
    hv = hash(key, nkey, 0);//获得分段锁信息，如果未进行扩容，则item的hash表是多个hash桶共用同一个锁，即是分段的锁
    item_lock(hv);//执行分段加锁
    it = do_item_get(key, nkey, hv);//执行get操作
    item_unlock(hv);//释放锁
    return it;
}

item *item_touch(const char *key, size_t nkey, uint32_t exptime) {
    item *it;
    uint32_t hv;
    hv = hash(key, nkey, 0);
    item_lock(hv);
    it = do_item_touch(key, nkey, exptime, hv);
    item_unlock(hv);
    return it;
}

/*
 * Links an item into the LRU and hashtable.
 */
int item_link(item *item) {
    int ret;
    uint32_t hv;

    hv = hash(ITEM_key(item), item->nkey, 0);
    item_lock(hv);
    ret = do_item_link(item, hv);
    item_unlock(hv);
    return ret;
}

/*
 * Decrements the reference count on an item and adds it to the freelist if
 * needed.
 */
void item_remove(item *item) {
    uint32_t hv;
    hv = hash(ITEM_key(item), item->nkey, 0);

    item_lock(hv);
    do_item_remove(item);
    item_unlock(hv);
}

/*
 * Replaces one item with another in the hashtable.
 * Unprotected by a mutex lock since the core server does not require
 * it to be thread-safe.
 */
int item_replace(item *old_it, item *new_it, const uint32_t hv) {
    return do_item_replace(old_it, new_it, hv);
}

/*
 * Unlinks an item from the LRU and hashtable.
 */
void item_unlink(item *item) {
    uint32_t hv;
    hv = hash(ITEM_key(item), item->nkey, 0);
    item_lock(hv);
    do_item_unlink(item, hv);
    item_unlock(hv);
}

/*
 * Moves an item to the back of the LRU queue.
 */
void item_update(item *item) {
    uint32_t hv;
    hv = hash(ITEM_key(item), item->nkey, 0);

    item_lock(hv);
    do_item_update(item);
    item_unlock(hv);
}

/*
 * Does arithmetic on a numeric item value.
 */
enum delta_result_type add_delta(conn *c, const char *key,
                                 const size_t nkey, int incr,
                                 const int64_t delta, char *buf,
                                 uint64_t *cas) {
    enum delta_result_type ret;
    uint32_t hv;

    hv = hash(key, nkey, 0);
    item_lock(hv);
    ret = do_add_delta(c, key, nkey, incr, delta, buf, cas, hv);
    item_unlock(hv);
    return ret;
}

/*
 * Stores an item in the cache (high level, obeys set/add/replace semantics)
 */
enum store_item_type store_item(item *item, int comm, conn* c) {
    enum store_item_type ret;
    uint32_t hv;

    hv = hash(ITEM_key(item), item->nkey, 0);//获取Hash表的分段锁
    item_lock(hv);//执行数据同步
    ret = do_store_item(item, comm, c, hv);//存储item
    item_unlock(hv);
    return ret;
}

/*
 * Flushes expired items after a flush_all call
 */
void item_flush_expired() {
    mutex_lock(&cache_lock);
    do_item_flush_expired();
    mutex_unlock(&cache_lock);
}

/*
 * Dumps part of the cache
 */
char *item_cachedump(unsigned int slabs_clsid, unsigned int limit, unsigned int *bytes) {
    char *ret;

    mutex_lock(&cache_lock);
    ret = do_item_cachedump(slabs_clsid, limit, bytes);
    mutex_unlock(&cache_lock);
    return ret;
}

/*
 * Dumps statistics about slab classes
 */
void  item_stats(ADD_STAT add_stats, void *c) {
    mutex_lock(&cache_lock);
    do_item_stats(add_stats, c);
    mutex_unlock(&cache_lock);
}

void  item_stats_totals(ADD_STAT add_stats, void *c) {
    mutex_lock(&cache_lock);
    do_item_stats_totals(add_stats, c);
    mutex_unlock(&cache_lock);
}

/*
 * Dumps a list of objects of each size in 32-byte increments
 */
void  item_stats_sizes(ADD_STAT add_stats, void *c) {
    mutex_lock(&cache_lock);
    do_item_stats_sizes(add_stats, c);
    mutex_unlock(&cache_lock);
}

/******************************* GLOBAL STATS ******************************/

void STATS_LOCK() {
    pthread_mutex_lock(&stats_lock);
}

void STATS_UNLOCK() {
    pthread_mutex_unlock(&stats_lock);
}

void threadlocal_stats_reset(void) {
    int ii, sid;
    for (ii = 0; ii < settings.num_threads; ++ii) {
        pthread_mutex_lock(&threads[ii].stats.mutex);

        threads[ii].stats.get_cmds = 0;
        threads[ii].stats.get_misses = 0;
        threads[ii].stats.touch_cmds = 0;
        threads[ii].stats.touch_misses = 0;
        threads[ii].stats.delete_misses = 0;
        threads[ii].stats.incr_misses = 0;
        threads[ii].stats.decr_misses = 0;
        threads[ii].stats.cas_misses = 0;
        threads[ii].stats.bytes_read = 0;
        threads[ii].stats.bytes_written = 0;
        threads[ii].stats.flush_cmds = 0;
        threads[ii].stats.conn_yields = 0;
        threads[ii].stats.auth_cmds = 0;
        threads[ii].stats.auth_errors = 0;

        for(sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
            threads[ii].stats.slab_stats[sid].set_cmds = 0;
            threads[ii].stats.slab_stats[sid].get_hits = 0;
            threads[ii].stats.slab_stats[sid].touch_hits = 0;
            threads[ii].stats.slab_stats[sid].delete_hits = 0;
            threads[ii].stats.slab_stats[sid].incr_hits = 0;
            threads[ii].stats.slab_stats[sid].decr_hits = 0;
            threads[ii].stats.slab_stats[sid].cas_hits = 0;
            threads[ii].stats.slab_stats[sid].cas_badval = 0;
        }

        pthread_mutex_unlock(&threads[ii].stats.mutex);
    }
}

void threadlocal_stats_aggregate(struct thread_stats *stats) {
    int ii, sid;

    /* The struct has a mutex, but we can safely set the whole thing
     * to zero since it is unused when aggregating. */
    memset(stats, 0, sizeof(*stats));

    for (ii = 0; ii < settings.num_threads; ++ii) {
        pthread_mutex_lock(&threads[ii].stats.mutex);

        stats->get_cmds += threads[ii].stats.get_cmds;
        stats->get_misses += threads[ii].stats.get_misses;
        stats->touch_cmds += threads[ii].stats.touch_cmds;
        stats->touch_misses += threads[ii].stats.touch_misses;
        stats->delete_misses += threads[ii].stats.delete_misses;
        stats->decr_misses += threads[ii].stats.decr_misses;
        stats->incr_misses += threads[ii].stats.incr_misses;
        stats->cas_misses += threads[ii].stats.cas_misses;
        stats->bytes_read += threads[ii].stats.bytes_read;
        stats->bytes_written += threads[ii].stats.bytes_written;
        stats->flush_cmds += threads[ii].stats.flush_cmds;
        stats->conn_yields += threads[ii].stats.conn_yields;
        stats->auth_cmds += threads[ii].stats.auth_cmds;
        stats->auth_errors += threads[ii].stats.auth_errors;

        for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
            stats->slab_stats[sid].set_cmds +=
                threads[ii].stats.slab_stats[sid].set_cmds;
            stats->slab_stats[sid].get_hits +=
                threads[ii].stats.slab_stats[sid].get_hits;
            stats->slab_stats[sid].touch_hits +=
                threads[ii].stats.slab_stats[sid].touch_hits;
            stats->slab_stats[sid].delete_hits +=
                threads[ii].stats.slab_stats[sid].delete_hits;
            stats->slab_stats[sid].decr_hits +=
                threads[ii].stats.slab_stats[sid].decr_hits;
            stats->slab_stats[sid].incr_hits +=
                threads[ii].stats.slab_stats[sid].incr_hits;
            stats->slab_stats[sid].cas_hits +=
                threads[ii].stats.slab_stats[sid].cas_hits;
            stats->slab_stats[sid].cas_badval +=
                threads[ii].stats.slab_stats[sid].cas_badval;
        }

        pthread_mutex_unlock(&threads[ii].stats.mutex);
    }
}

void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out) {
    int sid;

    out->set_cmds = 0;
    out->get_hits = 0;
    out->touch_hits = 0;
    out->delete_hits = 0;
    out->incr_hits = 0;
    out->decr_hits = 0;
    out->cas_hits = 0;
    out->cas_badval = 0;

    for (sid = 0; sid < MAX_NUMBER_OF_SLAB_CLASSES; sid++) {
        out->set_cmds += stats->slab_stats[sid].set_cmds;
        out->get_hits += stats->slab_stats[sid].get_hits;
        out->touch_hits += stats->slab_stats[sid].touch_hits;
        out->delete_hits += stats->slab_stats[sid].delete_hits;
        out->decr_hits += stats->slab_stats[sid].decr_hits;
        out->incr_hits += stats->slab_stats[sid].incr_hits;
        out->cas_hits += stats->slab_stats[sid].cas_hits;
        out->cas_badval += stats->slab_stats[sid].cas_badval;
    }
}

/* 工作线程初始化 , main_base: Event base for main thread */
void thread_init(int nthreads, struct event_base *main_base)
{
    int         i;
    int         power;
    //初始化各种锁和条件变量
    pthread_mutex_init(&cache_lock, NULL);
    pthread_mutex_init(&stats_lock, NULL);
    pthread_mutex_init(&init_lock, NULL);
    pthread_cond_init(&init_cond, NULL);
    pthread_mutex_init(&cqi_freelist_lock, NULL);
    cqi_freelist = NULL;
    //Memcached对hash桶的锁采用分段锁，按线程个数来分段，默认总共是1<<16个hash桶，而锁的数目是1<<power个
    if (nthreads < 3)
    {
        power = 10;
    }
    else if (nthreads < 4)
    {
        power = 11;
    }
    else if (nthreads < 5)
    {
        power = 12;
    }
    else
    {
        power = 13;/* 8192 buckets, and central locks don't scale much past 5 threads */
    }
    item_lock_count = hashsize(power);//锁的数目是1<<power
    //申请1<<power个pthread_mutex_t锁，保存在item_locks数组
    item_locks = calloc(item_lock_count, sizeof(pthread_mutex_t));
    if (! item_locks) {
        perror("Can't allocate item locks");
        exit(1);
    }
    //对这些锁进行初始化，这部分可参考APUE的线程部分
    for (i = 0; i < item_lock_count; i++)
    {
        pthread_mutex_init(&item_locks[i], NULL);
    }
    /*创建线程的局部变量，该局部变量的名称为item_lock_type_key,用于保存主hash表所持有的锁的类型.主hash表在进行扩容时，
    该锁类型会变为全局的锁，否则(不在扩容过程中)，则是局部锁*/
    pthread_key_create(&item_lock_type_key, NULL);//参考UNIX环境编程
    pthread_mutex_init(&item_global_lock, NULL);
    //申请nthreds个工作线程结构体,LIBEVENT_THREAD是Memcached内部对工作线程的一个封装
    threads = calloc(nthreads, sizeof(LIBEVENT_THREAD));
    if (! threads)
    {
        perror("Can't allocate thread descriptors");
        exit(1);
    }
    /*分发线程的初始化,分发线程的base为main_base线程,id为main线程的线程id*/
    dispatcher_thread.base = main_base;
    dispatcher_thread.thread_id = pthread_self();
    //工作线程的初始化,工作线程和主线程(main线程)是通过pipe管道进行通信的
    for (i = 0; i < nthreads; i++)
    {
        int fds[2];
        if (pipe(fds))
        {//初始化pipe管道
            perror("Can't create notify pipe");
            exit(1);
        }
        threads[i].notify_receive_fd = fds[0];//读管道绑定到工作线程的接收消息的描述符
        threads[i].notify_send_fd = fds[1];//写管道绑定到工作线程的发送消息的描述符
        setup_thread(&threads[i]);//把管道上的读写事件添加到libevent中
        /* Reserve three fds for the libevent base, and two for the pipe */
        stats.reserved_fds += 5;//统计信息更新
    }
    //创建工作线程，线程处理函数为worker_libevent
    for (i = 0; i < nthreads; i++)
    {
        create_worker(worker_libevent, &threads[i]);
    }
    //等待所有工作线程创建完毕main线程再return
    pthread_mutex_lock(&init_lock);
    wait_for_thread_registration(nthreads);
    pthread_mutex_unlock(&init_lock);
}

