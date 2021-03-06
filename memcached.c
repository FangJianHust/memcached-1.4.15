#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <signal.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <ctype.h>
#include <stdarg.h>

/* some POSIX systems need the following definition
 * to get mlockall flags out of sys/mman.h.  */
#ifndef _P1003_1B_VISIBLE
#define _P1003_1B_VISIBLE
#endif
/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif
#include <pwd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <limits.h>
#include <sysexits.h>
#include <stddef.h>

/* FreeBSD 4.x doesn't have IOV_MAX exposed. */
#ifndef IOV_MAX
#if defined(__FreeBSD__) || defined(__APPLE__)
# define IOV_MAX 1024
#endif
#endif

/*
 * forward declarations
 */
static void drive_machine(conn *c);
static int new_socket(struct addrinfo *ai);
static int try_read_command(conn *c);

enum try_read_result {
    READ_DATA_RECEIVED,
    READ_NO_DATA_RECEIVED,
    READ_ERROR,            /** an error occured (on the socket) (or client closed connection) */
    READ_MEMORY_ERROR      /** failed to allocate more memory */
};

static enum try_read_result try_read_network(conn *c);
static enum try_read_result try_read_udp(conn *c);

static void conn_set_state(conn *c, enum conn_states state);

/* stats */
static void stats_init(void);
static void server_stats(ADD_STAT add_stats, conn *c);
static void process_stat_settings(ADD_STAT add_stats, void *c);


/* defaults */
static void settings_init(void);

/* event handling, network IO */
static void event_handler(const int fd, const short which, void *arg);
static void conn_close(conn *c);
static void conn_init(void);
static bool update_event(conn *c, const int new_flags);
static void complete_nread(conn *c);
static void process_command(conn *c, char *command);
static void write_and_free(conn *c, char *buf, int bytes);
static int ensure_iov_space(conn *c);
static int add_iov(conn *c, const void *buf, int len);
static int add_msghdr(conn *c);


static void conn_free(conn *c);

/** exported globals **/
struct stats stats;
struct settings settings;
time_t process_started;     /* when the process was started */

struct slab_rebalance slab_rebal;
volatile int slab_rebalance_signal;

/** file scope variables **/
static conn *listen_conn = NULL;
static struct event_base *main_base;

enum transmit_result {
    TRANSMIT_COMPLETE,   /** All done writing. */
    TRANSMIT_INCOMPLETE, /** More data remaining to write. */
    TRANSMIT_SOFT_ERROR, /** Can't write any more right now. */
    TRANSMIT_HARD_ERROR  /** Can't write (c->state is set to conn_closing) */
};

static enum transmit_result transmit(conn *c);

/* This reduces the latency without adding lots of extra wiring to be able to
 * notify the listener thread of when to listen again.
 * Also, the clock timer could be broken out into its own thread and we
 * can block the listener via a condition.
 */
static volatile bool allow_new_conns = true;
static struct event maxconnsevent;
static void maxconns_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 0, .tv_usec = 10000};

    if (fd == -42 || allow_new_conns == false) {
        /* reschedule in 10ms if we need to keep polling */
        evtimer_set(&maxconnsevent, maxconns_handler, 0);
        event_base_set(main_base, &maxconnsevent);
        evtimer_add(&maxconnsevent, &t);
    } else {
        evtimer_del(&maxconnsevent);
        accept_new_conns(true);
    }
}

#define REALTIME_MAXDELTA 60*60*24*30

/*
 * given time value that's either unix time or delta from current unix time, return
 * unix time. Use the fact that delta can't exceed one month (and real time value can't
 * be that low).
 */
static rel_time_t realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + current_time);
    }
}

//对统计信息的结构体stat的初始化
static void stats_init(void)
{
    stats.curr_items = stats.total_items = stats.curr_conns = stats.total_conns = stats.conn_structs = 0;
    stats.get_cmds = stats.set_cmds = stats.get_hits = stats.get_misses = stats.evictions = stats.reclaimed = 0;
    stats.touch_cmds = stats.touch_misses = stats.touch_hits = stats.rejected_conns = 0;
    stats.curr_bytes = stats.listen_disabled_num = 0;
    stats.hash_power_level = stats.hash_bytes = stats.hash_is_expanding = 0;
    stats.expired_unfetched = stats.evicted_unfetched = 0;
    stats.slabs_moved = 0;
    stats.accepting_conns = true; /* assuming we start in this state. */
    stats.slab_reassign_running = false;
    /* make the time we started always be 2 seconds before we really did, so time(0) - time.started is never zero.
    if so, things like 'settings.oldest_live' which act as booleans as well as values are now false in boolean context */
    process_started = time(0) - 2;
    stats_prefix_init();
}

static void stats_reset(void) {
    STATS_LOCK();
    stats.total_items = stats.total_conns = 0;
    stats.rejected_conns = 0;
    stats.evictions = 0;
    stats.reclaimed = 0;
    stats.listen_disabled_num = 0;
    stats_prefix_clear();
    STATS_UNLOCK();
    threadlocal_stats_reset();
    item_stats_reset();
}

//命令行参数的初始化，即设置一些默认值，他们可能会在main函数中被修改
static void settings_init(void)
{
    settings.use_cas = true;
    settings.access = 0700;
    settings.port = 11211;
    settings.udpport = 11211;
    /* By default this string should be NULL for getaddrinfo() */
    settings.inter = NULL;
    settings.maxbytes = 64 * 1024 * 1024; /* default is 64MB */
    settings.maxconns = 1024;         /* to limit connections-related memory to about 5MB */
    settings.verbose = 0;
    settings.oldest_live = 0;
    settings.evict_to_free = 1;       /* push old items out of cache when memory runs out */
    settings.socketpath = NULL;       /* by default, not using a unix socket */
    settings.factor = 1.25;
    settings.chunk_size = 48;         /* space for a modest key and value */
    settings.num_threads = 4;         /* N workers */
    settings.num_threads_per_udp = 0;
    settings.prefix_delimiter = ':';
    settings.detail_enabled = 0;
    settings.reqs_per_event = 20;
    settings.backlog = 1024;
    settings.binding_protocol = negotiating_prot;
    settings.item_size_max = 1024 * 1024; /* The famous 1MB upper limit. */
    settings.maxconns_fast = false;
    settings.hashpower_init = 0;
    settings.slab_reassign = false;
    settings.slab_automove = 0;
}

/* Adds a message header to a connection.  Returns 0 on success, -1 on out-of-memory.*/
static int add_msghdr(conn *c)
{
    struct msghdr *msg;
    assert(c != NULL);
    if (c->msgsize == c->msgused) {
        msg = realloc(c->msglist, c->msgsize * 2 * sizeof(struct msghdr));
        if (! msg) return -1;
        c->msglist = msg;
        c->msgsize *= 2;
    }
    msg = c->msglist + c->msgused;
    /* this wipes msg_iovlen, msg_control, msg_controllen, and msg_flags, the last 3 of which aren't defined on solaris: */
    memset(msg, 0, sizeof(struct msghdr));
    msg->msg_iov = &c->iov[c->iovused];
    if (c->request_addr_size > 0) {
        msg->msg_name = &c->request_addr;
        msg->msg_namelen = c->request_addr_size;
    }
    c->msgbytes = 0;
    c->msgused++;
    if (IS_UDP(c->transport)) {
        /* Leave room for the UDP header, which we'll fill in later. */
        return add_iov(c, NULL, UDP_HEADER_SIZE);
    }
    return 0;
}

/* Free list management for connections.*/

static conn **freeconns;//空闲连接链表
static int freetotal;//空闲连接总数
static int freecurr;//当前空闲的索引
/* Lock for connection freelist */
static pthread_mutex_t conn_lock = PTHREAD_MUTEX_INITIALIZER;

//连接的初始化，申请200个连接池
static void conn_init(void)
{
    freetotal = 200;//空闲连接总数
    freecurr = 0;//当前空闲的索引
    //申请200个空间
    if ((freeconns = calloc(freetotal, sizeof(conn *))) == NULL)
    {
        fprintf(stderr, "Failed to allocate connection structures\n");
    }
    return;
}

/* Returns a connection from the freelist, if any.*/
conn *conn_from_freelist()
{
    conn *c;
    pthread_mutex_lock(&conn_lock);//操作链表，加锁，保持同步
    if (freecurr > 0) {//freecurr为静态全局变量
        c = freeconns[--freecurr];//freeconns是在Memcached启动时初始化的
    }
    else {//没有conn
        c = NULL;
    }
    pthread_mutex_unlock(&conn_lock);
    return c;
}

/* Adds a connection to the freelist. 0 = success. */
bool conn_add_to_freelist(conn *c)
{
    bool ret = true;
    pthread_mutex_lock(&conn_lock);
    if (freecurr < freetotal) {//freeconns还有空间
        freeconns[freecurr++] = c;//直接添加
        ret = false;
    }
    else
    {
        /* //没有多余空间，进行扩容，按目前容量的2倍进行扩容 */
        size_t newsize = freetotal * 2;
        conn **new_freeconns = realloc(freeconns, sizeof(conn *) * newsize);
        if (new_freeconns) {
            freetotal = newsize;
            freeconns = new_freeconns;
            freeconns[freecurr++] = c;
            ret = false;
        }
    }
    pthread_mutex_unlock(&conn_lock);
    return ret;
}

static const char *prot_text(enum protocol prot) {
    char *rv = "unknown";
    switch(prot) {
        case ascii_prot:
            rv = "ascii";
            break;
        case binary_prot:
            rv = "binary";
            break;
        case negotiating_prot:
            rv = "auto-negotiate";
            break;
    }
    return rv;
}

/* TCP的连接建立，UDP没有连接建立的过程，所以直接进行连接分发 */
conn *conn_new(const int sfd, enum conn_states init_state, const int event_flags,
                const int read_buffer_size, enum network_transport transport,struct event_base *base)
{
    conn *c = conn_from_freelist();//获取一个空闲连接，conn是Memcached内部对网络连接的一个封装
    //如果没有空闲的连接
    if (NULL == c)
    {
        if (!(c = (conn *)calloc(1, sizeof(conn))))
        {//申请空间
            fprintf(stderr, "calloc()\n");
            return NULL;
        }
        MEMCACHED_CONN_CREATE(c);
        //进行一些初始化
        c->rbuf = c->wbuf = 0;
        c->ilist = 0;
        c->suffixlist = 0;
        c->iov = 0;
        c->msglist = 0;
        c->hdrbuf = 0;
        c->rsize = read_buffer_size;
        c->wsize = DATA_BUFFER_SIZE;
        c->isize = ITEM_LIST_INITIAL;
        c->suffixsize = SUFFIX_LIST_INITIAL;
        c->iovsize = IOV_LIST_INITIAL;
        c->msgsize = MSG_LIST_INITIAL;
        c->hdrsize = 0;
        //每个conn都自带读入和输出缓冲区，在进行网络收发数据时，特别方便
        c->rbuf = (char *)malloc((size_t)c->rsize);
        c->wbuf = (char *)malloc((size_t)c->wsize);
        c->ilist = (item **)malloc(sizeof(item *) * c->isize);
        c->suffixlist = (char **)malloc(sizeof(char *) * c->suffixsize);
        c->iov = (struct iovec *)malloc(sizeof(struct iovec) * c->iovsize);
        c->msglist = (struct msghdr *)malloc(sizeof(struct msghdr) * c->msgsize);
        if (c->rbuf == 0 || c->wbuf == 0 || c->ilist == 0 || c->iov == 0 ||c->msglist == 0 || c->suffixlist == 0)
        {
            conn_free(c);
            fprintf(stderr, "malloc()\n");
            return NULL;
        }
        STATS_LOCK();
        stats.conn_structs++;//统计变量更新
        STATS_UNLOCK();
    }
    c->transport = transport;
    c->protocol = settings.binding_protocol;
    /* unix socket mode doesn't need this, so zeroed out.  but why is this done for every command?  presumably for UDP mode.  */
    if (!settings.socketpath)
    {
        c->request_addr_size = sizeof(c->request_addr);
    }
    else
    {
        c->request_addr_size = 0;
    }
    //输出一些日志信息
    if (settings.verbose > 1)
    {
        if (init_state == conn_listening) {
            fprintf(stderr, "<%d server listening (%s)\n", sfd,
                prot_text(c->protocol));
        } else if (IS_UDP(transport)) {
            fprintf(stderr, "<%d server listening (udp)\n", sfd);
        } else if (c->protocol == negotiating_prot) {
            fprintf(stderr, "<%d new auto-negotiating client connection\n",
                    sfd);
        } else if (c->protocol == ascii_prot) {
            fprintf(stderr, "<%d new ascii client connection.\n", sfd);
        } else if (c->protocol == binary_prot) {
            fprintf(stderr, "<%d new binary client connection.\n", sfd);
        } else {
            fprintf(stderr, "<%d new unknown (%d) client connection\n",sfd, c->protocol);
            assert(false);
        }
    }
    c->sfd = sfd;
    c->state = init_state;//设置连接状态
    c->rlbytes = 0;
    c->cmd = -1;
    c->rbytes = c->wbytes = 0;
    c->wcurr = c->wbuf;
    c->rcurr = c->rbuf;
    c->ritem = 0;
    c->icurr = c->ilist;
    c->suffixcurr = c->suffixlist;
    c->ileft = 0;
    c->suffixleft = 0;
    c->iovused = 0;
    c->msgcurr = 0;
    c->msgused = 0;
    c->write_and_go = init_state;
    c->write_and_free = 0;
    c->item = 0;
    c->noreply = false;
    //建立sfd描述符上面的event事件，读事件，事件回调函数为event_handler
    event_set(&c->event, sfd, event_flags, event_handler, (void *)c);
    event_base_set(base, &c->event);
    c->ev_flags = event_flags;
    if (event_add(&c->event, 0) == -1)
    {
    	//如果建立libevent事件失败，将创建的conn添加到空闲列表中
        if (conn_add_to_freelist(c))
        {
            conn_free(c);
        }
        perror("event_add");
        return NULL;
    }
    STATS_LOCK();
    stats.curr_conns++;//统计信息更新
    stats.total_conns++;
    STATS_UNLOCK();
    MEMCACHED_CONN_ALLOCATE(c->sfd);
    return c;
}

static void conn_cleanup(conn *c) {
    assert(c != NULL);

    if (c->item) {
        item_remove(c->item);
        c->item = 0;
    }

    if (c->ileft != 0) {
        for (; c->ileft > 0; c->ileft--,c->icurr++) {
            item_remove(*(c->icurr));
        }
    }

    if (c->suffixleft != 0) {
        for (; c->suffixleft > 0; c->suffixleft--, c->suffixcurr++) {
            cache_free(c->thread->suffix_cache, *(c->suffixcurr));
        }
    }

    if (c->write_and_free) {
        free(c->write_and_free);
        c->write_and_free = 0;
    }

    if (c->sasl_conn) {
        assert(settings.sasl);
        sasl_dispose(&c->sasl_conn);
        c->sasl_conn = NULL;
    }

    if (IS_UDP(c->transport)) {
        conn_set_state(c, conn_read);
    }
}

/*
 * Frees a connection.
 */
void conn_free(conn *c) {
    if (c) {
        MEMCACHED_CONN_DESTROY(c);
        if (c->hdrbuf)
            free(c->hdrbuf);
        if (c->msglist)
            free(c->msglist);
        if (c->rbuf)
            free(c->rbuf);
        if (c->wbuf)
            free(c->wbuf);
        if (c->ilist)
            free(c->ilist);
        if (c->suffixlist)
            free(c->suffixlist);
        if (c->iov)
            free(c->iov);
        free(c);
    }
}

static void conn_close(conn *c) {
    assert(c != NULL);

    /* delete the event, the socket and the conn */
    event_del(&c->event);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d connection closed.\n", c->sfd);

    MEMCACHED_CONN_RELEASE(c->sfd);
    close(c->sfd);
    pthread_mutex_lock(&conn_lock);
    allow_new_conns = true;
    pthread_mutex_unlock(&conn_lock);
    conn_cleanup(c);

    /* if the connection has big buffers, just free it */
    if (c->rsize > READ_BUFFER_HIGHWAT || conn_add_to_freelist(c)) {
        conn_free(c);
    }

    STATS_LOCK();
    stats.curr_conns--;
    STATS_UNLOCK();

    return;
}

/*
 * Shrinks(收缩) a connection's buffers if they're too big.  This prevents periodic(周期性的) large "get" requests from
 * permanently(永久地) chewing(咀嚼) lots of server memory.This should only be called in between requests since
 * it can wipe(擦去) output buffers!
 */
static void conn_shrink(conn *c)
{
    assert(c != NULL);
    if (IS_UDP(c->transport)) return;//如果是UDP协议，不牵涉缓冲区管理
    //读缓冲区空间大小>READ_BUFFER_HIGHWAT && 已经读到的还没解析的数据小于 DATA_BUFFER_SIZE
    if (c->rsize > READ_BUFFER_HIGHWAT && c->rbytes < DATA_BUFFER_SIZE)
    {
        char *newbuf;
        if (c->rcurr != c->rbuf)
            memmove(c->rbuf, c->rcurr, (size_t) c->rbytes);//目前数据是从rcurr开始的，移动数据到rbuf中
        newbuf = (char *) realloc((void *) c->rbuf, DATA_BUFFER_SIZE);//按DATA_BUFFER_SIZE扩大缓冲区
        if (newbuf)
        {
            c->rbuf = newbuf;//更新读缓冲区
            c->rsize = DATA_BUFFER_SIZE;//更新读缓冲区大小
        }
        c->rcurr = c->rbuf;
    }
    if (c->isize > ITEM_LIST_HIGHWAT)//需要写出的item的个数，也就是要发送给客户端的item的个数
    {
        item **newbuf = (item**) realloc((void *) c->ilist,ITEM_LIST_INITIAL * sizeof(c->ilist[0]));//增大存放item的空间
        if (newbuf)
        {
            c->ilist = newbuf;//更新信息
            c->isize = ITEM_LIST_INITIAL;//更新信息
        }
    }
    if (c->msgsize > MSG_LIST_HIGHWAT)//msghdr的个数，memcached发送消息是通过sendmsg批量发送的
    {
        struct msghdr *newbuf = (struct msghdr *) realloc((void *) c->msglist,MSG_LIST_INITIAL * sizeof(c->msglist[0]));//增大空间
        if (newbuf)
        {
            c->msglist = newbuf;//更新信息
            c->msgsize = MSG_LIST_INITIAL;//更新信息
        }
    }
    if (c->iovsize > IOV_LIST_HIGHWAT)//msghdr里面iov的数量
    {
        struct iovec *newbuf = (struct iovec *) realloc((void *) c->iov,IOV_LIST_INITIAL * sizeof(c->iov[0]));//增大空间
        if (newbuf)
        {
            c->iov = newbuf;//更新信息
            c->iovsize = IOV_LIST_INITIAL;//更新信息
        }
    }
}

/**
 * Convert a state name to a human readable form.
 */
static const char *state_text(enum conn_states state) {
    const char* const statenames[] = { "conn_listening",
                                       "conn_new_cmd",
                                       "conn_waiting",
                                       "conn_read",
                                       "conn_parse_cmd",
                                       "conn_write",
                                       "conn_nread",
                                       "conn_swallow",
                                       "conn_closing",
                                       "conn_mwrite" };
    return statenames[state];
}

/*
 * Sets a connection's current state in the state machine. Any special processing that needs to happen on certain
 * state transitions can happen here.
 */
static void conn_set_state(conn *c, enum conn_states state)
{
    assert(c != NULL);
    assert(state >= conn_listening && state < conn_max_state);
    if (state != c->state)
    {
        if (settings.verbose > 2)
        {
            fprintf(stderr, "%d: going from %s to %s\n",c->sfd, state_text(c->state),state_text(state));
        }
        if (state == conn_write || state == conn_mwrite)
        {
            MEMCACHED_PROCESS_COMMAND_END(c->sfd, c->wbuf, c->wbytes);
        }
        c->state = state;
    }
}

/*
 * Ensures that there is room for another struct iovec in a connection's
 * iov list.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */
static int ensure_iov_space(conn *c) {
    assert(c != NULL);

    if (c->iovused >= c->iovsize) {
        int i, iovnum;
        struct iovec *new_iov = (struct iovec *)realloc(c->iov,
                                (c->iovsize * 2) * sizeof(struct iovec));
        if (! new_iov)
            return -1;
        c->iov = new_iov;
        c->iovsize *= 2;

        /* Point all the msghdr structures at the new list. */
        for (i = 0, iovnum = 0; i < c->msgused; i++) {
            c->msglist[i].msg_iov = &c->iov[iovnum];
            iovnum += c->msglist[i].msg_iovlen;
        }
    }

    return 0;
}


/*
 * Adds data to the list of pending data that will be written out to a
 * connection.
 *
 * Returns 0 on success, -1 on out-of-memory.
 */

static int add_iov(conn *c, const void *buf, int len) {
    struct msghdr *m;
    int leftover;
    bool limit_to_mtu;

    assert(c != NULL);

    do {
        m = &c->msglist[c->msgused - 1];//指向最后一个消息结构体

        /*
         * Limit UDP packets, and the first payloads of TCP replies, to
         * UDP_MAX_PAYLOAD_SIZE bytes.
         */
        limit_to_mtu = IS_UDP(c->transport) || (1 == c->msgused);

        /* 具体参见UNIX网络编程P309 */
        if (m->msg_iovlen == IOV_MAX ||
            (limit_to_mtu && c->msgbytes >= UDP_MAX_PAYLOAD_SIZE)) {
            add_msghdr(c);
            m = &c->msglist[c->msgused - 1];
        }

        if (ensure_iov_space(c) != 0)
            return -1;

        /* If the fragment is too big to fit in the datagram, split it up */
        if (limit_to_mtu && len + c->msgbytes > UDP_MAX_PAYLOAD_SIZE)
        {
            leftover = len + c->msgbytes - UDP_MAX_PAYLOAD_SIZE;
            len -= leftover;
        }
        else
        {
            leftover = 0;
        }

        m = &c->msglist[c->msgused - 1];
        m->msg_iov[m->msg_iovlen].iov_base = (void *)buf;
        m->msg_iov[m->msg_iovlen].iov_len = len;

        c->msgbytes += len;
        c->iovused++;
        m->msg_iovlen++;

        buf = ((char *)buf) + len;
        len = leftover;
    } while (leftover > 0);

    return 0;
}


/*
 * Constructs a set of UDP headers and attaches them to the outgoing messages.
 */
static int build_udp_headers(conn *c) {
    int i;
    unsigned char *hdr;

    assert(c != NULL);

    if (c->msgused > c->hdrsize) {
        void *new_hdrbuf;
        if (c->hdrbuf)
            new_hdrbuf = realloc(c->hdrbuf, c->msgused * 2 * UDP_HEADER_SIZE);
        else
            new_hdrbuf = malloc(c->msgused * 2 * UDP_HEADER_SIZE);
        if (! new_hdrbuf)
            return -1;
        c->hdrbuf = (unsigned char *)new_hdrbuf;
        c->hdrsize = c->msgused * 2;
    }

    hdr = c->hdrbuf;
    for (i = 0; i < c->msgused; i++) {
        c->msglist[i].msg_iov[0].iov_base = (void*)hdr;
        c->msglist[i].msg_iov[0].iov_len = UDP_HEADER_SIZE;
        *hdr++ = c->request_id / 256;
        *hdr++ = c->request_id % 256;
        *hdr++ = i / 256;
        *hdr++ = i % 256;
        *hdr++ = c->msgused / 256;
        *hdr++ = c->msgused % 256;
        *hdr++ = 0;
        *hdr++ = 0;
        assert((void *) hdr == (caddr_t)c->msglist[i].msg_iov[0].iov_base + UDP_HEADER_SIZE);
    }

    return 0;
}


static void out_string(conn *c, const char *str) {
    size_t len;

    assert(c != NULL);

    if (c->noreply) {
        if (settings.verbose > 1)
            fprintf(stderr, ">%d NOREPLY %s\n", c->sfd, str);
        c->noreply = false;
        conn_set_state(c, conn_new_cmd);
        return;
    }

    if (settings.verbose > 1)
        fprintf(stderr, ">%d %s\n", c->sfd, str);

    /* Nuke a partial output... */
    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    add_msghdr(c);

    len = strlen(str);
    if ((len + 2) > c->wsize) {
        /* ought to be always enough. just fail for simplicity */
        str = "SERVER_ERROR output line too long";
        len = strlen(str);
    }

    memcpy(c->wbuf, str, len);
    memcpy(c->wbuf + len, "\r\n", 2);
    c->wbytes = len + 2;
    c->wcurr = c->wbuf;

    conn_set_state(c, conn_write);
    c->write_and_go = conn_new_cmd;
    return;
}

/* we get here after reading the value in set/add/replace commands. The command has been stored in c->cmd, and the
 * item is ready in c->item. */
static void complete_nread_ascii(conn *c)
{
    assert(c != NULL);
    item *it = c->item;
    int comm = c->cmd;
    enum store_item_type ret;
    pthread_mutex_lock(&c->thread->stats.mutex);
    c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    pthread_mutex_unlock(&c->thread->stats.mutex);
    if (strncmp(ITEM_data(it) + it->nbytes - 2, "\r\n", 2) != 0) {
        out_string(c, "CLIENT_ERROR bad data chunk");
    }
    else
    {
      ret = store_item(it, comm, c);
#ifdef ENABLE_DTRACE
      uint64_t cas = ITEM_get_cas(it);
      switch (c->cmd) {
      case NREAD_ADD:
          MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
                                (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_REPLACE:
          MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
                                    (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_APPEND:
          MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
                                   (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_PREPEND:
          MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
                                    (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_SET:
          MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
                                (ret == 1) ? it->nbytes : -1, cas);
          break;
      case NREAD_CAS:
          MEMCACHED_COMMAND_CAS(c->sfd, ITEM_key(it), it->nkey, it->nbytes,
                                cas);
          break;
      }
#endif
      switch (ret) {
      case STORED:
          out_string(c, "STORED");
          break;
      case EXISTS:
          out_string(c, "EXISTS");
          break;
      case NOT_FOUND:
          out_string(c, "NOT_FOUND");
          break;
      case NOT_STORED:
          out_string(c, "NOT_STORED");
          break;
      default:
          out_string(c, "SERVER_ERROR Unhandled storage type.");
      }
    }
    item_remove(c->item);       /* release the c->item reference */
    c->item = 0;
}

/**
 * get a pointer to the start of the request struct for the current command
 */
static void* binary_get_request(conn *c) {
    char *ret = c->rcurr;
    ret -= (sizeof(c->binary_header) + c->binary_header.request.keylen +
            c->binary_header.request.extlen);

    assert(ret >= c->rbuf);
    return ret;
}

/**
 * get a pointer to the key in this request
 */
static char* binary_get_key(conn *c) {
    return c->rcurr - (c->binary_header.request.keylen);
}

static void add_bin_header(conn *c, uint16_t err, uint8_t hdr_len, uint16_t key_len, uint32_t body_len) {
    protocol_binary_response_header* header;

    assert(c);

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        /* XXX:  out_string is inappropriate here */
        out_string(c, "SERVER_ERROR out of memory");
        return;
    }

    header = (protocol_binary_response_header *)c->wbuf;

    header->response.magic = (uint8_t)PROTOCOL_BINARY_RES;
    header->response.opcode = c->binary_header.request.opcode;
    header->response.keylen = (uint16_t)htons(key_len);

    header->response.extlen = (uint8_t)hdr_len;
    header->response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES;
    header->response.status = (uint16_t)htons(err);

    header->response.bodylen = htonl(body_len);
    header->response.opaque = c->opaque;
    header->response.cas = htonll(c->cas);

    if (settings.verbose > 1) {
        int ii;
        fprintf(stderr, ">%d Writing bin response:", c->sfd);
        for (ii = 0; ii < sizeof(header->bytes); ++ii) {
            if (ii % 4 == 0) {
                fprintf(stderr, "\n>%d  ", c->sfd);
            }
            fprintf(stderr, " 0x%02x", header->bytes[ii]);
        }
        fprintf(stderr, "\n");
    }

    add_iov(c, c->wbuf, sizeof(header->response));
}

static void write_bin_error(conn *c, protocol_binary_response_status err, int swallow) {
    const char *errstr = "Unknown error";
    size_t len;

    switch (err) {
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        errstr = "Out of memory";
        break;
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        errstr = "Unknown command";
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        errstr = "Not found";
        break;
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        errstr = "Invalid arguments";
        break;
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        errstr = "Data exists for key.";
        break;
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        errstr = "Too large.";
        break;
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        errstr = "Non-numeric server-side value for incr or decr";
        break;
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        errstr = "Not stored.";
        break;
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        errstr = "Auth failure.";
        break;
    default:
        assert(false);
        errstr = "UNHANDLED ERROR";
        fprintf(stderr, ">%d UNHANDLED ERROR: %d\n", c->sfd, err);
    }

    if (settings.verbose > 1) {
        fprintf(stderr, ">%d Writing an error: %s\n", c->sfd, errstr);
    }

    len = strlen(errstr);
    add_bin_header(c, err, 0, 0, len);
    if (len > 0) {
        add_iov(c, errstr, len);
    }
    conn_set_state(c, conn_mwrite);
    if(swallow > 0) {
        c->sbytes = swallow;
        c->write_and_go = conn_swallow;
    } else {
        c->write_and_go = conn_new_cmd;
    }
}

/* 发送一个二进制协议的响应报文 */
static void write_bin_response(conn *c, void *d, int hlen, int keylen, int dlen)
{
    if (!c->noreply || c->cmd == PROTOCOL_BINARY_CMD_GET ||c->cmd == PROTOCOL_BINARY_CMD_GETK)
    {
        add_bin_header(c, 0, hlen, keylen, dlen);//头部
        if(dlen > 0)
        {
            add_iov(c, d, dlen);//包体
        }
        conn_set_state(c, conn_mwrite);//进入conn_mwrite状态
        c->write_and_go = conn_new_cmd;
    }
    else
    {
        conn_set_state(c, conn_new_cmd);
    }
}

static void complete_incr_bin(conn *c) {
    item *it;
    char *key;
    size_t nkey;
    /* Weird magic in add_delta forces me to pad here */
    char tmpbuf[INCR_MAX_STORAGE_LEN];
    uint64_t cas = 0;

    protocol_binary_response_incr* rsp = (protocol_binary_response_incr*)c->wbuf;
    protocol_binary_request_incr* req = binary_get_request(c);

    assert(c != NULL);
    assert(c->wsize >= sizeof(*rsp));

    /* fix byteorder in the request */
    req->message.body.delta = ntohll(req->message.body.delta);
    req->message.body.initial = ntohll(req->message.body.initial);
    req->message.body.expiration = ntohl(req->message.body.expiration);
    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        int i;
        fprintf(stderr, "incr ");

        for (i = 0; i < nkey; i++) {
            fprintf(stderr, "%c", key[i]);
        }
        fprintf(stderr, " %lld, %llu, %d\n",
                (long long)req->message.body.delta,
                (long long)req->message.body.initial,
                req->message.body.expiration);
    }

    if (c->binary_header.request.cas != 0) {
        cas = c->binary_header.request.cas;
    }
    switch(add_delta(c, key, nkey, c->cmd == PROTOCOL_BINARY_CMD_INCREMENT,
                     req->message.body.delta, tmpbuf,
                     &cas)) {
    case OK:
        rsp->message.body.value = htonll(strtoull(tmpbuf, NULL, 10));
        if (cas) {
            c->cas = cas;
        }
        write_bin_response(c, &rsp->message.body, 0, 0,
                           sizeof(rsp->message.body.value));
        break;
    case NON_NUMERIC:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL, 0);
        break;
    case EOM:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
        break;
    case DELTA_ITEM_NOT_FOUND:
        if (req->message.body.expiration != 0xffffffff) {
            /* Save some room for the response */
            rsp->message.body.value = htonll(req->message.body.initial);
            it = item_alloc(key, nkey, 0, realtime(req->message.body.expiration),
                            INCR_MAX_STORAGE_LEN);

            if (it != NULL) {
                snprintf(ITEM_data(it), INCR_MAX_STORAGE_LEN, "%llu",
                         (unsigned long long)req->message.body.initial);

                if (store_item(it, NREAD_ADD, c)) {
                    c->cas = ITEM_get_cas(it);
                    write_bin_response(c, &rsp->message.body, 0, 0, sizeof(rsp->message.body.value));
                } else {
                    write_bin_error(c, PROTOCOL_BINARY_RESPONSE_NOT_STORED, 0);
                }
                item_remove(it);         /* release our reference */
            } else {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
            }
        } else {
            pthread_mutex_lock(&c->thread->stats.mutex);
            if (c->cmd == PROTOCOL_BINARY_CMD_INCREMENT) {
                c->thread->stats.incr_misses++;
            } else {
                c->thread->stats.decr_misses++;
            }
            pthread_mutex_unlock(&c->thread->stats.mutex);

            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        }
        break;
    case DELTA_ITEM_CAS_MISMATCH:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    }
}

//二进制的set命令处理方法
static void complete_update_bin(conn *c)
{
    protocol_binary_response_status eno = PROTOCOL_BINARY_RESPONSE_EINVAL;
    enum store_item_type ret = NOT_STORED;
    assert(c != NULL);

    item *it = c->item;

    pthread_mutex_lock(&c->thread->stats.mutex);
    c->thread->stats.slab_stats[it->slabs_clsid].set_cmds++;
    pthread_mutex_unlock(&c->thread->stats.mutex);

    /* We don't actually receive the trailing two characters in the bin
     * protocol, so we're going to just set them here */
    *(ITEM_data(it) + it->nbytes - 2) = '\r';
    *(ITEM_data(it) + it->nbytes - 1) = '\n';

    //存储item
    ret = store_item(it, c->cmd, c);

#ifdef ENABLE_DTRACE
    uint64_t cas = ITEM_get_cas(it);
    switch (c->cmd) {
    case NREAD_ADD:
        MEMCACHED_COMMAND_ADD(c->sfd, ITEM_key(it), it->nkey,
                              (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_REPLACE:
        MEMCACHED_COMMAND_REPLACE(c->sfd, ITEM_key(it), it->nkey,
                                  (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_APPEND:
        MEMCACHED_COMMAND_APPEND(c->sfd, ITEM_key(it), it->nkey,
                                 (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_PREPEND:
        MEMCACHED_COMMAND_PREPEND(c->sfd, ITEM_key(it), it->nkey,
                                 (ret == STORED) ? it->nbytes : -1, cas);
        break;
    case NREAD_SET:
        MEMCACHED_COMMAND_SET(c->sfd, ITEM_key(it), it->nkey,
                              (ret == STORED) ? it->nbytes : -1, cas);
        break;
    }
#endif

    switch (ret)
    {
    case STORED://存储成功
        /* Stored */
        write_bin_response(c, NULL, 0, 0, 0);
        break;
    case EXISTS:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        break;
    case NOT_FOUND:
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        break;
    case NOT_STORED:
        if (c->cmd == NREAD_ADD)
        {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS;
        }
        else if(c->cmd == NREAD_REPLACE)
        {
            eno = PROTOCOL_BINARY_RESPONSE_KEY_ENOENT;
        }
        else
        {
            eno = PROTOCOL_BINARY_RESPONSE_NOT_STORED;
        }
        write_bin_error(c, eno, 0);
    }

    item_remove(c->item);       /* release the c->item reference */
    c->item = 0;
}

static void process_bin_touch(conn *c) {
    item *it;

    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    protocol_binary_request_touch *t = binary_get_request(c);
    time_t exptime = ntohl(t->message.body.expiration);

    if (settings.verbose > 1) {
        int ii;
        /* May be GAT/GATQ/etc */
        fprintf(stderr, "<%d TOUCH ", c->sfd);
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, "\n");
    }

    it = item_touch(key, nkey, realtime(exptime));

    if (it) {
        /* the length has two unnecessary bytes ("\r\n") */
        uint16_t keylen = 0;
        uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);

        item_update(it);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.touch_cmds++;
        c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        MEMCACHED_COMMAND_TOUCH(c->sfd, ITEM_key(it), it->nkey,
                                it->nbytes, ITEM_get_cas(it));

        if (c->cmd == PROTOCOL_BINARY_CMD_TOUCH) {
            bodylen -= it->nbytes - 2;
        } else if (c->cmd == PROTOCOL_BINARY_CMD_GATK) {
            bodylen += nkey;
            keylen = nkey;
        }

        add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
        rsp->message.header.response.cas = htonll(ITEM_get_cas(it));

        // add the flags
        rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));

        if (c->cmd == PROTOCOL_BINARY_CMD_GATK) {
            add_iov(c, ITEM_key(it), nkey);
        }

        /* Add the data minus the CRLF */
        if (c->cmd != PROTOCOL_BINARY_CMD_TOUCH) {
            add_iov(c, ITEM_data(it), it->nbytes - 2);
        }

        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        /* Remember this command so we can garbage collect it later */
        c->item = it;
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.touch_cmds++;
        c->thread->stats.touch_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        MEMCACHED_COMMAND_TOUCH(c->sfd, key, nkey, -1, 0);

        if (c->noreply) {
            conn_set_state(c, conn_new_cmd);
        } else {
            if (c->cmd == PROTOCOL_BINARY_CMD_GATK) {
                char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
                add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,
                        0, nkey, nkey);
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, nkey);
                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;
            } else {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            }
        }
    }

    if (settings.detail_enabled) {
        stats_prefix_record_get(key, nkey, NULL != it);
    }
}

//二进制的get命令处理方法
static void process_bin_get(conn *c)
{
    item *it;
    protocol_binary_response_get* rsp = (protocol_binary_response_get*)c->wbuf;
    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;
    if (settings.verbose > 1) {
        int ii;
        fprintf(stderr, "<%d GET ", c->sfd);
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }
        fprintf(stderr, "\n");
    }
    it = item_get(key, nkey);//根据key信息和key的长度信息获取相应的item
    //如果找到item
    if (it)
    {
        /* the length has two unnecessary bytes ("\r\n") */
        uint16_t keylen = 0;
        uint32_t bodylen = sizeof(rsp->message.body) + (it->nbytes - 2);
        item_update(it);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.get_cmds++;
        c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
        MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,it->nbytes, ITEM_get_cas(it));
        if (c->cmd == PROTOCOL_BINARY_CMD_GETK)
        {
            bodylen += nkey;
            keylen = nkey;
        }
        //添加协议头部
        add_bin_header(c, 0, sizeof(rsp->message.body), keylen, bodylen);
        rsp->message.header.response.cas = htonll(ITEM_get_cas(it));
        // add the flags
        rsp->message.body.flags = htonl(strtoul(ITEM_suffix(it), NULL, 10));
        //添加数据到msghdr结构体
        add_iov(c, &rsp->message.body, sizeof(rsp->message.body));
        if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
            add_iov(c, ITEM_key(it), nkey);
        }
        /* Add the data minus the CRLF */
        add_iov(c, ITEM_data(it), it->nbytes - 2);
        //进入发送状态
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        /* Remember this command so we can garbage collect it later */
        c->item = it;
    }
    else
    {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.get_cmds++;
        c->thread->stats.get_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
        MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
        if (c->noreply)
        {
            conn_set_state(c, conn_new_cmd);
        }
        else
        {
            if (c->cmd == PROTOCOL_BINARY_CMD_GETK) {
                char *ofs = c->wbuf + sizeof(protocol_binary_response_header);
                add_bin_header(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0, nkey, nkey);
                memcpy(ofs, key, nkey);
                add_iov(c, ofs, nkey);
                conn_set_state(c, conn_mwrite);
                c->write_and_go = conn_new_cmd;
            } else {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            }
        }
    }
    if (settings.detail_enabled) {
        stats_prefix_record_get(key, nkey, NULL != it);
    }
}

static void append_bin_stats(const char *key, const uint16_t klen,
                             const char *val, const uint32_t vlen,
                             conn *c) {
    char *buf = c->stats.buffer + c->stats.offset;
    uint32_t bodylen = klen + vlen;
    protocol_binary_response_header header = {
        .response.magic = (uint8_t)PROTOCOL_BINARY_RES,
        .response.opcode = PROTOCOL_BINARY_CMD_STAT,
        .response.keylen = (uint16_t)htons(klen),
        .response.datatype = (uint8_t)PROTOCOL_BINARY_RAW_BYTES,
        .response.bodylen = htonl(bodylen),
        .response.opaque = c->opaque
    };

    memcpy(buf, header.bytes, sizeof(header.response));
    buf += sizeof(header.response);

    if (klen > 0) {
        memcpy(buf, key, klen);
        buf += klen;

        if (vlen > 0) {
            memcpy(buf, val, vlen);
        }
    }

    c->stats.offset += sizeof(header.response) + bodylen;
}

static void append_ascii_stats(const char *key, const uint16_t klen,
                               const char *val, const uint32_t vlen,
                               conn *c) {
    char *pos = c->stats.buffer + c->stats.offset;
    uint32_t nbytes = 0;
    int remaining = c->stats.size - c->stats.offset;
    int room = remaining - 1;

    if (klen == 0 && vlen == 0) {
        nbytes = snprintf(pos, room, "END\r\n");
    } else if (vlen == 0) {
        nbytes = snprintf(pos, room, "STAT %s\r\n", key);
    } else {
        nbytes = snprintf(pos, room, "STAT %s %s\r\n", key, val);
    }

    c->stats.offset += nbytes;
}

static bool grow_stats_buf(conn *c, size_t needed) {
    size_t nsize = c->stats.size;
    size_t available = nsize - c->stats.offset;
    bool rv = true;

    /* Special case: No buffer -- need to allocate fresh */
    if (c->stats.buffer == NULL) {
        nsize = 1024;
        available = c->stats.size = c->stats.offset = 0;
    }

    while (needed > available) {
        assert(nsize > 0);
        nsize = nsize << 1;
        available = nsize - c->stats.offset;
    }

    if (nsize != c->stats.size) {
        char *ptr = realloc(c->stats.buffer, nsize);
        if (ptr) {
            c->stats.buffer = ptr;
            c->stats.size = nsize;
        } else {
            rv = false;
        }
    }

    return rv;
}

static void append_stats(const char *key, const uint16_t klen,
                  const char *val, const uint32_t vlen,
                  const void *cookie)
{
    /* value without a key is invalid */
    if (klen == 0 && vlen > 0) {
        return ;
    }

    conn *c = (conn*)cookie;

    if (c->protocol == binary_prot) {
        size_t needed = vlen + klen + sizeof(protocol_binary_response_header);
        if (!grow_stats_buf(c, needed)) {
            return ;
        }
        append_bin_stats(key, klen, val, vlen, c);
    } else {
        size_t needed = vlen + klen + 10; // 10 == "STAT = \r\n"
        if (!grow_stats_buf(c, needed)) {
            return ;
        }
        append_ascii_stats(key, klen, val, vlen, c);
    }

    assert(c->stats.offset <= c->stats.size);
}

static void process_bin_stat(conn *c) {
    char *subcommand = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    if (settings.verbose > 1) {
        int ii;
        fprintf(stderr, "<%d STATS ", c->sfd);
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", subcommand[ii]);
        }
        fprintf(stderr, "\n");
    }

    if (nkey == 0) {
        /* request all statistics */
        server_stats(&append_stats, c);
        (void)get_stats(NULL, 0, &append_stats, c);
    } else if (strncmp(subcommand, "reset", 5) == 0) {
        stats_reset();
    } else if (strncmp(subcommand, "settings", 8) == 0) {
        process_stat_settings(&append_stats, c);
    } else if (strncmp(subcommand, "detail", 6) == 0) {
        char *subcmd_pos = subcommand + 6;
        if (strncmp(subcmd_pos, " dump", 5) == 0) {
            int len;
            char *dump_buf = stats_prefix_dump(&len);
            if (dump_buf == NULL || len <= 0) {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
                return ;
            } else {
                append_stats("detailed", strlen("detailed"), dump_buf, len, c);
                free(dump_buf);
            }
        } else if (strncmp(subcmd_pos, " on", 3) == 0) {
            settings.detail_enabled = 1;
        } else if (strncmp(subcmd_pos, " off", 4) == 0) {
            settings.detail_enabled = 0;
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
            return;
        }
    } else {
        if (get_stats(subcommand, nkey, &append_stats, c)) {
            if (c->stats.buffer == NULL) {
                write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
            } else {
                write_and_free(c, c->stats.buffer, c->stats.offset);
                c->stats.buffer = NULL;
            }
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        }

        return;
    }

    /* Append termination package and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);
    if (c->stats.buffer == NULL) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, 0);
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

static void bin_read_key(conn *c, enum bin_substates next_substate, int extra) {
    assert(c);
    c->substate = next_substate;
    c->rlbytes = c->keylen + extra;

    /* Ok... do we have room for the extras and the key in the input buffer? */
    ptrdiff_t offset = c->rcurr + sizeof(protocol_binary_request_header) - c->rbuf;
    if (c->rlbytes > c->rsize - offset) {
        size_t nsize = c->rsize;
        size_t size = c->rlbytes + sizeof(protocol_binary_request_header);

        while (size > nsize) {
            nsize *= 2;
        }

        if (nsize != c->rsize) {
            if (settings.verbose > 1) {
                fprintf(stderr, "%d: Need to grow buffer from %lu to %lu\n",
                        c->sfd, (unsigned long)c->rsize, (unsigned long)nsize);
            }
            char *newm = realloc(c->rbuf, nsize);
            if (newm == NULL) {
                if (settings.verbose) {
                    fprintf(stderr, "%d: Failed to grow buffer.. closing connection\n",
                            c->sfd);
                }
                conn_set_state(c, conn_closing);
                return;
            }

            c->rbuf= newm;
            /* rcurr should point to the same offset in the packet */
            c->rcurr = c->rbuf + offset - sizeof(protocol_binary_request_header);
            c->rsize = nsize;
        }
        if (c->rbuf != c->rcurr) {
            memmove(c->rbuf, c->rcurr, c->rbytes);
            c->rcurr = c->rbuf;
            if (settings.verbose > 1) {
                fprintf(stderr, "%d: Repack input buffer\n", c->sfd);
            }
        }
    }

    /* preserve the header in the buffer.. */
    c->ritem = c->rcurr + sizeof(protocol_binary_request_header);
    conn_set_state(c, conn_nread);
}

/* Just write an error message and disconnect the client */
static void handle_binary_protocol_error(conn *c) {
    write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, 0);
    if (settings.verbose) {
        fprintf(stderr, "Protocol error (opcode %02x), close connection %d\n",
                c->binary_header.request.opcode, c->sfd);
    }
    c->write_and_go = conn_closing;
}

static void init_sasl_conn(conn *c) {
    assert(c);
    /* should something else be returned? */
    if (!settings.sasl)
        return;

    if (!c->sasl_conn) {
        int result=sasl_server_new("memcached",
                                   NULL,
                                   my_sasl_hostname[0] ? my_sasl_hostname : NULL,
                                   NULL, NULL,
                                   NULL, 0, &c->sasl_conn);
        if (result != SASL_OK) {
            if (settings.verbose) {
                fprintf(stderr, "Failed to initialize SASL conn.\n");
            }
            c->sasl_conn = NULL;
        }
    }
}

static void bin_list_sasl_mechs(conn *c) {
    // Guard against a disabled SASL.
    if (!settings.sasl) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,
                        c->binary_header.request.bodylen
                        - c->binary_header.request.keylen);
        return;
    }

    init_sasl_conn(c);
    const char *result_string = NULL;
    unsigned int string_length = 0;
    int result=sasl_listmech(c->sasl_conn, NULL,
                             "",   /* What to prepend the string with */
                             " ",  /* What to separate mechanisms with */
                             "",   /* What to append to the string */
                             &result_string, &string_length,
                             NULL);
    if (result != SASL_OK) {
        /* Perhaps there's a better error for this... */
        if (settings.verbose) {
            fprintf(stderr, "Failed to list SASL mechanisms.\n");
        }
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        return;
    }
    write_bin_response(c, (char*)result_string, 0, 0, string_length);
}

static void process_bin_sasl_auth(conn *c) {
    // Guard for handling disabled SASL on the server.
    if (!settings.sasl) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,
                        c->binary_header.request.bodylen
                        - c->binary_header.request.keylen);
        return;
    }

    assert(c->binary_header.request.extlen == 0);

    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    if (nkey > MAX_SASL_MECH_LEN) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_EINVAL, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    char *key = binary_get_key(c);
    assert(key);

    item *it = item_alloc(key, nkey, 0, 0, vlen);

    if (it == 0) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        c->write_and_go = conn_swallow;
        return;
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_reading_sasl_auth_data;
}

static void process_bin_complete_sasl_auth(conn *c) {
    assert(settings.sasl);
    const char *out = NULL;
    unsigned int outlen = 0;

    assert(c->item);
    init_sasl_conn(c);

    int nkey = c->binary_header.request.keylen;
    int vlen = c->binary_header.request.bodylen - nkey;

    char mech[nkey+1];
    memcpy(mech, ITEM_key((item*)c->item), nkey);
    mech[nkey] = 0x00;

    if (settings.verbose)
        fprintf(stderr, "mech:  ``%s'' with %d bytes of data\n", mech, vlen);

    const char *challenge = vlen == 0 ? NULL : ITEM_data((item*) c->item);

    int result=-1;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_AUTH:
        result = sasl_server_start(c->sasl_conn, mech,
                                   challenge, vlen,
                                   &out, &outlen);
        break;
    case PROTOCOL_BINARY_CMD_SASL_STEP:
        result = sasl_server_step(c->sasl_conn,
                                  challenge, vlen,
                                  &out, &outlen);
        break;
    default:
        assert(false); /* CMD should be one of the above */
        /* This code is pretty much impossible, but makes the compiler
           happier */
        if (settings.verbose) {
            fprintf(stderr, "Unhandled command %d with challenge %s\n",
                    c->cmd, challenge);
        }
        break;
    }

    item_unlink(c->item);

    if (settings.verbose) {
        fprintf(stderr, "sasl result code:  %d\n", result);
    }

    switch(result) {
    case SASL_OK:
        write_bin_response(c, "Authenticated", 0, 0, strlen("Authenticated"));
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.auth_cmds++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
        break;
    case SASL_CONTINUE:
        add_bin_header(c, PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE, 0, 0, outlen);
        if(outlen > 0) {
            add_iov(c, out, outlen);
        }
        conn_set_state(c, conn_mwrite);
        c->write_and_go = conn_new_cmd;
        break;
    default:
        if (settings.verbose)
            fprintf(stderr, "Unknown sasl response:  %d\n", result);
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.auth_cmds++;
        c->thread->stats.auth_errors++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
    }
}

static bool authenticated(conn *c) {
    assert(settings.sasl);
    bool rv = false;

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS: /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_AUTH:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_SASL_STEP:       /* FALLTHROUGH */
    case PROTOCOL_BINARY_CMD_VERSION:         /* FALLTHROUGH */
        rv = true;
        break;
    default:
        if (c->sasl_conn) {
            const void *uname = NULL;
            sasl_getprop(c->sasl_conn, SASL_USERNAME, &uname);
            rv = uname != NULL;
        }
    }

    if (settings.verbose > 1) {
        fprintf(stderr, "authenticated() in cmd 0x%02x is %s\n",
                c->cmd, rv ? "true" : "false");
    }

    return rv;
}

/* 处理具体的（比如get,set等）操作的，和是二进制协议具体相关的 */
static void dispatch_bin_command(conn *c) {
    int protocol_error = 0;

    int extlen = c->binary_header.request.extlen;
    int keylen = c->binary_header.request.keylen;
    uint32_t bodylen = c->binary_header.request.bodylen;

    if (settings.sasl && !authenticated(c)) {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_AUTH_ERROR, 0);
        c->write_and_go = conn_closing;
        return;
    }

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);
    c->noreply = true;

    /* binprot supports 16bit keys, but internals are still 8bit */
    if (keylen > KEY_MAX_LENGTH) {
        handle_binary_protocol_error(c);
        return;
    }

    switch (c->cmd) {
    case PROTOCOL_BINARY_CMD_SETQ:
        c->cmd = PROTOCOL_BINARY_CMD_SET;
        break;
    case PROTOCOL_BINARY_CMD_ADDQ:
        c->cmd = PROTOCOL_BINARY_CMD_ADD;
        break;
    case PROTOCOL_BINARY_CMD_REPLACEQ:
        c->cmd = PROTOCOL_BINARY_CMD_REPLACE;
        break;
    case PROTOCOL_BINARY_CMD_DELETEQ:
        c->cmd = PROTOCOL_BINARY_CMD_DELETE;
        break;
    case PROTOCOL_BINARY_CMD_INCREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_INCREMENT;
        break;
    case PROTOCOL_BINARY_CMD_DECREMENTQ:
        c->cmd = PROTOCOL_BINARY_CMD_DECREMENT;
        break;
    case PROTOCOL_BINARY_CMD_QUITQ:
        c->cmd = PROTOCOL_BINARY_CMD_QUIT;
        break;
    case PROTOCOL_BINARY_CMD_FLUSHQ:
        c->cmd = PROTOCOL_BINARY_CMD_FLUSH;
        break;
    case PROTOCOL_BINARY_CMD_APPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_APPEND;
        break;
    case PROTOCOL_BINARY_CMD_PREPENDQ:
        c->cmd = PROTOCOL_BINARY_CMD_PREPEND;
        break;
    case PROTOCOL_BINARY_CMD_GETQ:
        c->cmd = PROTOCOL_BINARY_CMD_GET;
        break;
    case PROTOCOL_BINARY_CMD_GETKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GETK;
        break;
    case PROTOCOL_BINARY_CMD_GATQ:
        c->cmd = PROTOCOL_BINARY_CMD_GAT;
        break;
    case PROTOCOL_BINARY_CMD_GATKQ:
        c->cmd = PROTOCOL_BINARY_CMD_GAT;
        break;
    default:
        c->noreply = false;
    }

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_VERSION:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, VERSION, 0, 0, strlen(VERSION));
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_FLUSH:
            if (keylen == 0 && bodylen == extlen && (extlen == 0 || extlen == 4)) {
                bin_read_key(c, bin_read_flush_exptime, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_NOOP:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SET: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_ADD: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_REPLACE:
            if (extlen == 8 && keylen != 0 && bodylen >= (keylen + 8)) {
                bin_read_key(c, bin_reading_set_header, 8);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_GETQ:  /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GET:   /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETKQ: /* FALLTHROUGH */
        case PROTOCOL_BINARY_CMD_GETK:
            if (extlen == 0 && bodylen == keylen && keylen > 0) {
                bin_read_key(c, bin_reading_get_key, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_DELETE:
            if (keylen > 0 && extlen == 0 && bodylen == keylen) {
                bin_read_key(c, bin_reading_del_header, extlen);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_INCREMENT:
        case PROTOCOL_BINARY_CMD_DECREMENT:
            if (keylen > 0 && extlen == 20 && bodylen == (keylen + extlen)) {
                bin_read_key(c, bin_reading_incr_header, 20);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_APPEND:
        case PROTOCOL_BINARY_CMD_PREPEND:
            if (keylen > 0 && extlen == 0) {
                bin_read_key(c, bin_reading_set_header, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_STAT:
            if (extlen == 0) {
                bin_read_key(c, bin_reading_stat, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_QUIT:
            if (keylen == 0 && extlen == 0 && bodylen == 0) {
                write_bin_response(c, NULL, 0, 0, 0);
                c->write_and_go = conn_closing;
                if (c->noreply) {
                    conn_set_state(c, conn_closing);
                }
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_LIST_MECHS:
            if (extlen == 0 && keylen == 0 && bodylen == 0) {
                bin_list_sasl_mechs(c);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_SASL_AUTH:
        case PROTOCOL_BINARY_CMD_SASL_STEP:
            if (extlen == 0 && keylen != 0) {
                bin_read_key(c, bin_reading_sasl_auth, 0);
            } else {
                protocol_error = 1;
            }
            break;
        case PROTOCOL_BINARY_CMD_TOUCH:
        case PROTOCOL_BINARY_CMD_GAT:
        case PROTOCOL_BINARY_CMD_GATQ:
        case PROTOCOL_BINARY_CMD_GATK:
        case PROTOCOL_BINARY_CMD_GATKQ:
            if (extlen == 4 && keylen != 0) {
                bin_read_key(c, bin_reading_touch_key, 4);
            } else {
                protocol_error = 1;
            }
            break;
        default:
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, bodylen);
    }

    if (protocol_error)
        handle_binary_protocol_error(c);
}

static void process_bin_update(conn *c) {
    char *key;
    int nkey;
    int vlen;
    item *it;
    protocol_binary_request_set* req = binary_get_request(c);

    assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;

    /* fix byteorder in the request */
    req->message.body.flags = ntohl(req->message.body.flags);
    req->message.body.expiration = ntohl(req->message.body.expiration);

    vlen = c->binary_header.request.bodylen - (nkey + c->binary_header.request.extlen);

    if (settings.verbose > 1) {
        int ii;
        if (c->cmd == PROTOCOL_BINARY_CMD_ADD) {
            fprintf(stderr, "<%d ADD ", c->sfd);
        } else if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            fprintf(stderr, "<%d SET ", c->sfd);
        } else {
            fprintf(stderr, "<%d REPLACE ", c->sfd);
        }
        for (ii = 0; ii < nkey; ++ii) {
            fprintf(stderr, "%c", key[ii]);
        }

        fprintf(stderr, " Value len is %d", vlen);
        fprintf(stderr, "\n");
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, req->message.body.flags,
            realtime(req->message.body.expiration), vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, req->message.body.flags, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (c->cmd == PROTOCOL_BINARY_CMD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_ADD:
            c->cmd = NREAD_ADD;
            break;
        case PROTOCOL_BINARY_CMD_SET:
            c->cmd = NREAD_SET;
            break;
        case PROTOCOL_BINARY_CMD_REPLACE:
            c->cmd = NREAD_REPLACE;
            break;
        default:
            assert(0);
    }

    if (ITEM_get_cas(it) != 0) {
        c->cmd = NREAD_CAS;
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_append_prepend(conn *c) {
    char *key;
    int nkey;
    int vlen;
    item *it;

    assert(c != NULL);

    key = binary_get_key(c);
    nkey = c->binary_header.request.keylen;
    vlen = c->binary_header.request.bodylen - nkey;

    if (settings.verbose > 1) {
        fprintf(stderr, "Value len is %d\n", vlen);
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, 0, 0, vlen+2);

    if (it == 0) {
        if (! item_size_ok(nkey, 0, vlen + 2)) {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_E2BIG, vlen);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_ENOMEM, vlen);
        }
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        return;
    }

    ITEM_set_cas(it, c->binary_header.request.cas);

    switch (c->cmd) {
        case PROTOCOL_BINARY_CMD_APPEND:
            c->cmd = NREAD_APPEND;
            break;
        case PROTOCOL_BINARY_CMD_PREPEND:
            c->cmd = NREAD_PREPEND;
            break;
        default:
            assert(0);
    }

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = vlen;
    conn_set_state(c, conn_nread);
    c->substate = bin_read_set_value;
}

static void process_bin_flush(conn *c) {
    time_t exptime = 0;
    protocol_binary_request_flush* req = binary_get_request(c);

    if (c->binary_header.request.extlen == sizeof(req->message.body)) {
        exptime = ntohl(req->message.body.expiration);
    }

    if (exptime > 0) {
        settings.oldest_live = realtime(exptime) - 1;
    } else {
        settings.oldest_live = current_time - 1;
    }
    item_flush_expired();

    pthread_mutex_lock(&c->thread->stats.mutex);
    c->thread->stats.flush_cmds++;
    pthread_mutex_unlock(&c->thread->stats.mutex);

    write_bin_response(c, NULL, 0, 0, 0);
}

static void process_bin_delete(conn *c) {
    item *it;

    protocol_binary_request_delete* req = binary_get_request(c);

    char* key = binary_get_key(c);
    size_t nkey = c->binary_header.request.keylen;

    assert(c != NULL);

    if (settings.verbose > 1) {
        fprintf(stderr, "Deleting %s\n", key);
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        uint64_t cas = ntohll(req->message.header.request.cas);
        if (cas == 0 || cas == ITEM_get_cas(it)) {
            MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
            item_unlink(it);
            write_bin_response(c, NULL, 0, 0, 0);
        } else {
            write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS, 0);
        }
        item_remove(it);      /* release our reference */
    } else {
        write_bin_error(c, PROTOCOL_BINARY_RESPONSE_KEY_ENOENT, 0);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.delete_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);
    }
}

//二进制协议根据不同的命令调用不同的方法进行处理
static void complete_nread_binary(conn *c)
{
    assert(c != NULL);
    assert(c->cmd >= 0);

    switch(c->substate)
    {
    case bin_reading_set_header:
        if (c->cmd == PROTOCOL_BINARY_CMD_APPEND ||
                c->cmd == PROTOCOL_BINARY_CMD_PREPEND) {
            process_bin_append_prepend(c);
        } else {
            process_bin_update(c);
        }
        break;
    case bin_read_set_value:
        complete_update_bin(c);//set命令
        break;
    case bin_reading_get_key:
        process_bin_get(c);//get命令
        break;
    case bin_reading_touch_key:
        process_bin_touch(c);
        break;
    case bin_reading_stat:
        process_bin_stat(c);
        break;
    case bin_reading_del_header:
        process_bin_delete(c);
        break;
    case bin_reading_incr_header:
        complete_incr_bin(c);
        break;
    case bin_read_flush_exptime:
        process_bin_flush(c);
        break;
    case bin_reading_sasl_auth:
        process_bin_sasl_auth(c);
        break;
    case bin_reading_sasl_auth_data:
        process_bin_complete_sasl_auth(c);
        break;
    default:
        fprintf(stderr, "Not handling substate %d\n", c->substate);
        assert(0);
    }
}

//整理缓冲区
static void reset_cmd_handler(conn *c)
{
    c->cmd = -1;
    c->substate = bin_no_state;
    if (c->item != NULL)//还有item
    {
        item_remove(c->item);
        c->item = NULL;
    }
    conn_shrink(c);//整理缓冲区
    if (c->rbytes > 0)//缓冲区还有数据
    {
        conn_set_state(c, conn_parse_cmd);//有数据需要处理，进入更新状态
    }
    else//如果没有数据
    {
        conn_set_state(c, conn_waiting);//进入等待状态，状态机没有数据要处理，就进入这个状态
    }
}

//conn_nread状态读取完后执行
static void complete_nread(conn *c)
{
    assert(c != NULL);
    assert(c->protocol == ascii_prot || c->protocol == binary_prot);
    if (c->protocol == ascii_prot) {
        complete_nread_ascii(c);
    }
    else if (c->protocol == binary_prot)
    {
        complete_nread_binary(c);
    }
}

/*
 * Stores an item in the cache according to the semantics of one of the set
 * commands. In threaded mode, this is protected by the cache lock.
 *
 * Returns the state of storage.
 */
enum store_item_type do_store_item(item *it, int comm, conn *c,const uint32_t hv)
{
    char *key = ITEM_key(it);//读取item对应的key
    item *old_it = do_item_get(key, it->nkey, hv);//读取相应的item,如果没有相关的数据,old_it为NULL
    enum store_item_type stored = NOT_STORED;//item状态标记

    item *new_it = NULL;
    int flags;

    if (old_it != NULL && comm == NREAD_ADD)//如果old_it不为NULL,且操作为add操作
    {
        do_item_update(old_it);//更新数据
    }
    //old_it为空，且操作为REPLACE，则什么都不做
    else if (!old_it  && (comm == NREAD_REPLACE || comm == NREAD_APPEND  || comm == NREAD_PREPEND))
    {
          //memcached的Replace操作是替换已有的数据，如果没有相关数据，则不做任何操作
    }
    //以cas方式读取
    else if (comm == NREAD_CAS)
    {
        if (old_it == NULL) //为空
        {
            // LRU expired
            stored = NOT_FOUND;//修改状态
            pthread_mutex_lock(&c->thread->stats.mutex);//更新Worker线程统计数据
            c->thread->stats.cas_misses++;
            pthread_mutex_unlock(&c->thread->stats.mutex);
        }
        else if (ITEM_get_cas(it) == ITEM_get_cas(old_it))//old_it不为NULL，且cas属性一致
        {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_hits++;//更新Worker线程统计信息
            pthread_mutex_unlock(&c->thread->stats.mutex);

            item_replace(old_it, it, hv);//执行item的替换操作，用新的item替换老的item
            stored = STORED;//修改状态值
        }
        else//old_it不为NULL,且cas属性不一致
        {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.slab_stats[old_it->slabs_clsid].cas_badval++;//更新Worker线程统计信息
            pthread_mutex_unlock(&c->thread->stats.mutex);

            if (settings.verbose > 1)
            {
                fprintf(stderr, "CAS:  failure: expected %llu, got %llu\n",
                        (unsigned long long) ITEM_get_cas(old_it),
                        (unsigned long long) ITEM_get_cas(it));
            }
            stored = EXISTS;//修改状态值，修改状态值为已经存在，且不存储最新的数据
        }
    }
    else //执行其他操作的写
    {
        if (comm == NREAD_APPEND || comm == NREAD_PREPEND)//以追加的方式执行写
        {

            if (ITEM_get_cas(it) != 0)//验证cas有效性
            {
                if (ITEM_get_cas(it) != ITEM_get_cas(old_it))//cas验证不通过
                {
                    stored = EXISTS;//修改状态值为已存在
                }
            }

            if (stored == NOT_STORED)//状态值为没有存储,也就是cas验证通过，则执行写操作
            {
                flags = (int) strtol(ITEM_suffix(old_it), (char **) NULL, 10);
                //申请新的空间
                new_it = do_item_alloc(key, it->nkey, flags, old_it->exptime,it->nbytes + old_it->nbytes - 2 , hv);

                if (new_it == NULL)
                {
                    //空间不足
                    if (old_it != NULL)
                        do_item_remove(old_it);//删除老的item

                    return NOT_STORED;
                }

                if (comm == NREAD_APPEND)//追加方式
                {
                    memcpy(ITEM_data(new_it), ITEM_data(old_it),old_it->nbytes);//老数据拷贝到新数据中
                    memcpy(ITEM_data(new_it) + old_it->nbytes - 2,ITEM_data(it), it->nbytes);//同时拷贝最近缓冲区已有的数据
                }
                else
                {
                    //这里和具体协议相关
                    memcpy(ITEM_data(new_it), ITEM_data(it), it->nbytes);//拷贝it的数据到new_it中
                    memcpy(ITEM_data(new_it) + it->nbytes - 2 ,ITEM_data(old_it), old_it->nbytes);//同时拷贝最近缓冲区已有的数据
                }

                it = new_it;
            }
        }

        if (stored == NOT_STORED)
        {
            if (old_it != NULL)//如果old_it不为空
                item_replace(old_it, it, hv);//替换老的值
            else
                do_item_link(it, hv);//重新存储数据

            c->cas = ITEM_get_cas(it);//获取cas值

            stored = STORED;
        }
    }

    if (old_it != NULL)
        do_item_remove(old_it);//释放空间
    if (new_it != NULL)
        do_item_remove(new_it);//释放空间

    if (stored == STORED)//如果已经存储了
    {
        c->cas = ITEM_get_cas(it);//获取cas属性
    }

    return stored;
}

typedef struct token_s {
    char *value;
    size_t length;
} token_t;

#define COMMAND_TOKEN 0
#define SUBCOMMAND_TOKEN 1
#define KEY_TOKEN 1

#define MAX_TOKENS 8

/*
 * Tokenize the command string by replacing whitespace with '\0' and update
 * the token array tokens with pointer to start of each token and length.
 * Returns total number of tokens.  The last valid token is the terminal
 * token (value points to the first unprocessed character of the string and
 * length zero).
 *
 * Usage example:
 *
 *  while(tokenize_command(command, ncommand, tokens, max_tokens) > 0) {
 *      for(int ix = 0; tokens[ix].length != 0; ix++) {
 *          ...
 *      }
 *      ncommand = tokens[ix].value - command;
 *      command  = tokens[ix].value;
 *   }
 */
static size_t tokenize_command(char *command, token_t *tokens, const size_t max_tokens) {
    char *s, *e;
    size_t ntokens = 0;
    size_t len = strlen(command);
    unsigned int i = 0;

    assert(command != NULL && tokens != NULL && max_tokens > 1);

    s = e = command;
    for (i = 0; i < len; i++) {
        if (*e == ' ') {
            if (s != e) {
                tokens[ntokens].value = s;
                tokens[ntokens].length = e - s;
                ntokens++;
                *e = '\0';
                if (ntokens == max_tokens - 1) {
                    e++;
                    s = e; /* so we don't add an extra token */
                    break;
                }
            }
            s = e + 1;
        }
        e++;
    }

    if (s != e) {
        tokens[ntokens].value = s;
        tokens[ntokens].length = e - s;
        ntokens++;
    }

    /*
     * If we scanned the whole string, the terminal value pointer is null,
     * otherwise it is the first unprocessed character.
     */
    tokens[ntokens].value =  *e == '\0' ? NULL : e;
    tokens[ntokens].length = 0;
    ntokens++;

    return ntokens;
}

/* set up a connection to write a buffer then free it, used for stats */
static void write_and_free(conn *c, char *buf, int bytes) {
    if (buf) {
        c->write_and_free = buf;
        c->wcurr = buf;
        c->wbytes = bytes;
        conn_set_state(c, conn_write);
        c->write_and_go = conn_new_cmd;
    } else {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    }
}

static inline bool set_noreply_maybe(conn *c, token_t *tokens, size_t ntokens)
{
    int noreply_index = ntokens - 2;

    /*
      NOTE: this function is not the first place where we are going to
      send the reply.  We could send it instead from process_command()
      if the request line has wrong number of tokens.  However parsing
      malformed line for "noreply" option is not reliable anyway, so
      it can't be helped.
    */
    if (tokens[noreply_index].value
        && strcmp(tokens[noreply_index].value, "noreply") == 0) {
        c->noreply = true;
    }
    return c->noreply;
}

void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...) {
    char val_str[STAT_VAL_LEN];
    int vlen;
    va_list ap;

    assert(name);
    assert(add_stats);
    assert(c);
    assert(fmt);

    va_start(ap, fmt);
    vlen = vsnprintf(val_str, sizeof(val_str) - 1, fmt, ap);
    va_end(ap);

    add_stats(name, strlen(name), val_str, vlen, c);
}

inline static void process_stats_detail(conn *c, const char *command) {
    assert(c != NULL);

    if (strcmp(command, "on") == 0) {
        settings.detail_enabled = 1;
        out_string(c, "OK");
    }
    else if (strcmp(command, "off") == 0) {
        settings.detail_enabled = 0;
        out_string(c, "OK");
    }
    else if (strcmp(command, "dump") == 0) {
        int len;
        char *stats = stats_prefix_dump(&len);
        write_and_free(c, stats, len);
    }
    else {
        out_string(c, "CLIENT_ERROR usage: stats detail on|off|dump");
    }
}

/* return server specific stats only */
static void server_stats(ADD_STAT add_stats, conn *c) {
    pid_t pid = getpid();
    rel_time_t now = current_time;

    struct thread_stats thread_stats;
    threadlocal_stats_aggregate(&thread_stats);
    struct slab_stats slab_stats;
    slab_stats_aggregate(&thread_stats, &slab_stats);

#ifndef WIN32
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
#endif /* !WIN32 */

    STATS_LOCK();

    APPEND_STAT("pid", "%lu", (long)pid);
    APPEND_STAT("uptime", "%u", now);
    APPEND_STAT("time", "%ld", now + (long)process_started);
    APPEND_STAT("version", "%s", VERSION);
    APPEND_STAT("libevent", "%s", event_get_version());
    APPEND_STAT("pointer_size", "%d", (int)(8 * sizeof(void *)));

#ifndef WIN32
    append_stat("rusage_user", add_stats, c, "%ld.%06ld",
                (long)usage.ru_utime.tv_sec,
                (long)usage.ru_utime.tv_usec);
    append_stat("rusage_system", add_stats, c, "%ld.%06ld",
                (long)usage.ru_stime.tv_sec,
                (long)usage.ru_stime.tv_usec);
#endif /* !WIN32 */

    APPEND_STAT("curr_connections", "%u", stats.curr_conns - 1);
    APPEND_STAT("total_connections", "%u", stats.total_conns);
    if (settings.maxconns_fast) {
        APPEND_STAT("rejected_connections", "%llu", (unsigned long long)stats.rejected_conns);
    }
    APPEND_STAT("connection_structures", "%u", stats.conn_structs);
    APPEND_STAT("reserved_fds", "%u", stats.reserved_fds);
    APPEND_STAT("cmd_get", "%llu", (unsigned long long)thread_stats.get_cmds);
    APPEND_STAT("cmd_set", "%llu", (unsigned long long)slab_stats.set_cmds);
    APPEND_STAT("cmd_flush", "%llu", (unsigned long long)thread_stats.flush_cmds);
    APPEND_STAT("cmd_touch", "%llu", (unsigned long long)thread_stats.touch_cmds);
    APPEND_STAT("get_hits", "%llu", (unsigned long long)slab_stats.get_hits);
    APPEND_STAT("get_misses", "%llu", (unsigned long long)thread_stats.get_misses);
    APPEND_STAT("delete_misses", "%llu", (unsigned long long)thread_stats.delete_misses);
    APPEND_STAT("delete_hits", "%llu", (unsigned long long)slab_stats.delete_hits);
    APPEND_STAT("incr_misses", "%llu", (unsigned long long)thread_stats.incr_misses);
    APPEND_STAT("incr_hits", "%llu", (unsigned long long)slab_stats.incr_hits);
    APPEND_STAT("decr_misses", "%llu", (unsigned long long)thread_stats.decr_misses);
    APPEND_STAT("decr_hits", "%llu", (unsigned long long)slab_stats.decr_hits);
    APPEND_STAT("cas_misses", "%llu", (unsigned long long)thread_stats.cas_misses);
    APPEND_STAT("cas_hits", "%llu", (unsigned long long)slab_stats.cas_hits);
    APPEND_STAT("cas_badval", "%llu", (unsigned long long)slab_stats.cas_badval);
    APPEND_STAT("touch_hits", "%llu", (unsigned long long)slab_stats.touch_hits);
    APPEND_STAT("touch_misses", "%llu", (unsigned long long)thread_stats.touch_misses);
    APPEND_STAT("auth_cmds", "%llu", (unsigned long long)thread_stats.auth_cmds);
    APPEND_STAT("auth_errors", "%llu", (unsigned long long)thread_stats.auth_errors);
    APPEND_STAT("bytes_read", "%llu", (unsigned long long)thread_stats.bytes_read);
    APPEND_STAT("bytes_written", "%llu", (unsigned long long)thread_stats.bytes_written);
    APPEND_STAT("limit_maxbytes", "%llu", (unsigned long long)settings.maxbytes);
    APPEND_STAT("accepting_conns", "%u", stats.accepting_conns);
    APPEND_STAT("listen_disabled_num", "%llu", (unsigned long long)stats.listen_disabled_num);
    APPEND_STAT("threads", "%d", settings.num_threads);
    APPEND_STAT("conn_yields", "%llu", (unsigned long long)thread_stats.conn_yields);
    APPEND_STAT("hash_power_level", "%u", stats.hash_power_level);
    APPEND_STAT("hash_bytes", "%llu", (unsigned long long)stats.hash_bytes);
    APPEND_STAT("hash_is_expanding", "%u", stats.hash_is_expanding);
    if (settings.slab_reassign) {
        APPEND_STAT("slab_reassign_running", "%u", stats.slab_reassign_running);
        APPEND_STAT("slabs_moved", "%llu", stats.slabs_moved);
    }
    STATS_UNLOCK();
}

static void process_stat_settings(ADD_STAT add_stats, void *c) {
    assert(add_stats);
    APPEND_STAT("maxbytes", "%u", (unsigned int)settings.maxbytes);
    APPEND_STAT("maxconns", "%d", settings.maxconns);
    APPEND_STAT("tcpport", "%d", settings.port);
    APPEND_STAT("udpport", "%d", settings.udpport);
    APPEND_STAT("inter", "%s", settings.inter ? settings.inter : "NULL");
    APPEND_STAT("verbosity", "%d", settings.verbose);
    APPEND_STAT("oldest", "%lu", (unsigned long)settings.oldest_live);
    APPEND_STAT("evictions", "%s", settings.evict_to_free ? "on" : "off");
    APPEND_STAT("domain_socket", "%s",
                settings.socketpath ? settings.socketpath : "NULL");
    APPEND_STAT("umask", "%o", settings.access);
    APPEND_STAT("growth_factor", "%.2f", settings.factor);
    APPEND_STAT("chunk_size", "%d", settings.chunk_size);
    APPEND_STAT("num_threads", "%d", settings.num_threads);
    APPEND_STAT("num_threads_per_udp", "%d", settings.num_threads_per_udp);
    APPEND_STAT("stat_key_prefix", "%c", settings.prefix_delimiter);
    APPEND_STAT("detail_enabled", "%s",
                settings.detail_enabled ? "yes" : "no");
    APPEND_STAT("reqs_per_event", "%d", settings.reqs_per_event);
    APPEND_STAT("cas_enabled", "%s", settings.use_cas ? "yes" : "no");
    APPEND_STAT("tcp_backlog", "%d", settings.backlog);
    APPEND_STAT("binding_protocol", "%s",
                prot_text(settings.binding_protocol));
    APPEND_STAT("auth_enabled_sasl", "%s", settings.sasl ? "yes" : "no");
    APPEND_STAT("item_size_max", "%d", settings.item_size_max);
    APPEND_STAT("maxconns_fast", "%s", settings.maxconns_fast ? "yes" : "no");
    APPEND_STAT("hashpower_init", "%d", settings.hashpower_init);
    APPEND_STAT("slab_reassign", "%s", settings.slab_reassign ? "yes" : "no");
    APPEND_STAT("slab_automove", "%d", settings.slab_automove);
}

static void process_stat(conn *c, token_t *tokens, const size_t ntokens) {
    const char *subcommand = tokens[SUBCOMMAND_TOKEN].value;
    assert(c != NULL);

    if (ntokens < 2) {
        out_string(c, "CLIENT_ERROR bad command line");
        return;
    }

    if (ntokens == 2) {
        server_stats(&append_stats, c);
        (void)get_stats(NULL, 0, &append_stats, c);
    } else if (strcmp(subcommand, "reset") == 0) {
        stats_reset();
        out_string(c, "RESET");
        return ;
    } else if (strcmp(subcommand, "detail") == 0) {
        /* NOTE: how to tackle detail with binary? */
        if (ntokens < 4)
            process_stats_detail(c, "");  /* outputs the error message */
        else
            process_stats_detail(c, tokens[2].value);
        /* Output already generated */
        return ;
    } else if (strcmp(subcommand, "settings") == 0) {
        process_stat_settings(&append_stats, c);
    } else if (strcmp(subcommand, "cachedump") == 0) {
        char *buf;
        unsigned int bytes, id, limit = 0;

        if (ntokens < 5) {
            out_string(c, "CLIENT_ERROR bad command line");
            return;
        }

        if (!safe_strtoul(tokens[2].value, &id) ||
            !safe_strtoul(tokens[3].value, &limit)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        if (id >= POWER_LARGEST) {
            out_string(c, "CLIENT_ERROR Illegal slab id");
            return;
        }

        buf = item_cachedump(id, limit, &bytes);
        write_and_free(c, buf, bytes);
        return ;
    } else {
        /* getting here means that the subcommand is either engine specific or
           is invalid. query the engine and see. */
        if (get_stats(subcommand, strlen(subcommand), &append_stats, c)) {
            if (c->stats.buffer == NULL) {
                out_string(c, "SERVER_ERROR out of memory writing stats");
            } else {
                write_and_free(c, c->stats.buffer, c->stats.offset);
                c->stats.buffer = NULL;
            }
        } else {
            out_string(c, "ERROR");
        }
        return ;
    }

    /* append terminator and start the transfer */
    append_stats(NULL, 0, NULL, 0, c);

    if (c->stats.buffer == NULL) {
        out_string(c, "SERVER_ERROR out of memory writing stats");
    } else {
        write_and_free(c, c->stats.buffer, c->stats.offset);
        c->stats.buffer = NULL;
    }
}

/* ntokens is overwritten here... shrug.. */
static inline void process_get_command(conn *c, token_t *tokens, size_t ntokens, bool return_cas) {
    char *key;
    size_t nkey;
    int i = 0;
    item *it;
    token_t *key_token = &tokens[KEY_TOKEN];
    char *suffix;
    assert(c != NULL);

    do {
        while(key_token->length != 0) {

            key = key_token->value;
            nkey = key_token->length;

            if(nkey > KEY_MAX_LENGTH) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            it = item_get(key, nkey);
            if (settings.detail_enabled) {
                stats_prefix_record_get(key, nkey, NULL != it);
            }
            if (it) {
                if (i >= c->isize) {
                    item **new_list = realloc(c->ilist, sizeof(item *) * c->isize * 2);
                    if (new_list) {
                        c->isize *= 2;
                        c->ilist = new_list;
                    } else {
                        item_remove(it);
                        break;
                    }
                }

                /*
                 * Construct the response. Each hit adds three elements to the
                 * outgoing data list:
                 *   "VALUE "
                 *   key
                 *   " " + flags + " " + data length + "\r\n" + data (with \r\n)
                 */

                if (return_cas)
                {
                  MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                        it->nbytes, ITEM_get_cas(it));
                  /* Goofy mid-flight realloc. */
                  if (i >= c->suffixsize) {
                    char **new_suffix_list = realloc(c->suffixlist,
                                           sizeof(char *) * c->suffixsize * 2);
                    if (new_suffix_list) {
                        c->suffixsize *= 2;
                        c->suffixlist  = new_suffix_list;
                    } else {
                        item_remove(it);
                        break;
                    }
                  }

                  suffix = cache_alloc(c->thread->suffix_cache);
                  if (suffix == NULL) {
                    out_string(c, "SERVER_ERROR out of memory making CAS suffix");
                    item_remove(it);
                    return;
                  }
                  *(c->suffixlist + i) = suffix;
                  int suffix_len = snprintf(suffix, SUFFIX_SIZE,
                                            " %llu\r\n",
                                            (unsigned long long)ITEM_get_cas(it));
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, ITEM_key(it), it->nkey) != 0 ||
                      add_iov(c, ITEM_suffix(it), it->nsuffix - 2) != 0 ||
                      add_iov(c, suffix, suffix_len) != 0 ||
                      add_iov(c, ITEM_data(it), it->nbytes) != 0)
                      {
                          item_remove(it);
                          break;
                      }
                }
                else
                {
                  MEMCACHED_COMMAND_GET(c->sfd, ITEM_key(it), it->nkey,
                                        it->nbytes, ITEM_get_cas(it));
                  if (add_iov(c, "VALUE ", 6) != 0 ||
                      add_iov(c, ITEM_key(it), it->nkey) != 0 ||
                      add_iov(c, ITEM_suffix(it), it->nsuffix + it->nbytes) != 0)
                      {
                          item_remove(it);
                          break;
                      }
                }


                if (settings.verbose > 1)
                    fprintf(stderr, ">%d sending key %s\n", c->sfd, ITEM_key(it));

                /* item_get() has incremented it->refcount for us */
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.slab_stats[it->slabs_clsid].get_hits++;
                c->thread->stats.get_cmds++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                item_update(it);
                *(c->ilist + i) = it;
                i++;

            } else {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.get_misses++;
                c->thread->stats.get_cmds++;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                MEMCACHED_COMMAND_GET(c->sfd, key, nkey, -1, 0);
            }

            key_token++;
        }

        /*
         * If the command string hasn't been fully processed, get the next set
         * of tokens.
         */
        if(key_token->value != NULL) {
            ntokens = tokenize_command(key_token->value, tokens, MAX_TOKENS);
            key_token = tokens;
        }

    } while(key_token->value != NULL);

    c->icurr = c->ilist;
    c->ileft = i;
    if (return_cas) {
        c->suffixcurr = c->suffixlist;
        c->suffixleft = i;
    }

    if (settings.verbose > 1)
        fprintf(stderr, ">%d END\n", c->sfd);

    /*
        If the loop was terminated because of out-of-memory, it is not
        reliable to add END\r\n to the buffer, because it might not end
        in \r\n. So we send SERVER_ERROR instead.
    */
    if (key_token->value != NULL || add_iov(c, "END\r\n", 5) != 0
        || (IS_UDP(c->transport) && build_udp_headers(c) != 0)) {
        out_string(c, "SERVER_ERROR out of memory writing get response");
    }
    else {
        conn_set_state(c, conn_mwrite);
        c->msgcurr = 0;
    }

    return;
}

static void process_update_command(conn *c, token_t *tokens, const size_t ntokens, int comm, bool handle_cas) {
    char *key;
    size_t nkey;
    unsigned int flags;
    int32_t exptime_int = 0;
    time_t exptime;
    int vlen;
    uint64_t req_cas_id=0;
    item *it;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (! (safe_strtoul(tokens[2].value, (uint32_t *)&flags)
           && safe_strtol(tokens[3].value, &exptime_int)
           && safe_strtol(tokens[4].value, (int32_t *)&vlen))) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    /* Ubuntu 8.04 breaks when I pass exptime to safe_strtol */
    exptime = exptime_int;

    /* Negative exptimes can underflow and end up immortal. realtime() will
       immediately expire values that are greater than REALTIME_MAXDELTA, but less
       than process_started, so lets aim for that. */
    if (exptime < 0)
        exptime = REALTIME_MAXDELTA + 1;

    // does cas value exist?
    if (handle_cas) {
        if (!safe_strtoull(tokens[5].value, &req_cas_id)) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }
    }

    vlen += 2;
    if (vlen < 0 || vlen - 2 < 0) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_set(key, nkey);
    }

    it = item_alloc(key, nkey, flags, realtime(exptime), vlen);

    if (it == 0) {
        if (! item_size_ok(nkey, flags, vlen))
            out_string(c, "SERVER_ERROR object too large for cache");
        else
            out_string(c, "SERVER_ERROR out of memory storing object");
        /* swallow the data line */
        c->write_and_go = conn_swallow;
        c->sbytes = vlen;

        /* Avoid stale data persisting in cache because we failed alloc.
         * Unacceptable for SET. Anywhere else too? */
        if (comm == NREAD_SET) {
            it = item_get(key, nkey);
            if (it) {
                item_unlink(it);
                item_remove(it);
            }
        }

        return;
    }
    ITEM_set_cas(it, req_cas_id);

    c->item = it;
    c->ritem = ITEM_data(it);
    c->rlbytes = it->nbytes;
    c->cmd = comm;
    conn_set_state(c, conn_nread);
}

static void process_touch_command(conn *c, token_t *tokens, const size_t ntokens) {
    char *key;
    size_t nkey;
    int32_t exptime_int = 0;
    item *it;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (!safe_strtol(tokens[2].value, &exptime_int)) {
        out_string(c, "CLIENT_ERROR invalid exptime argument");
        return;
    }

    it = item_touch(key, nkey, realtime(exptime_int));
    if (it) {
        item_update(it);
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.touch_cmds++;
        c->thread->stats.slab_stats[it->slabs_clsid].touch_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "TOUCHED");
        item_remove(it);
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.touch_cmds++;
        c->thread->stats.touch_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
    }
}

static void process_arithmetic_command(conn *c, token_t *tokens, const size_t ntokens, const bool incr) {
    char temp[INCR_MAX_STORAGE_LEN];
    uint64_t delta;
    char *key;
    size_t nkey;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    if (tokens[KEY_TOKEN].length > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if (!safe_strtoull(tokens[2].value, &delta)) {
        out_string(c, "CLIENT_ERROR invalid numeric delta argument");
        return;
    }

    switch(add_delta(c, key, nkey, incr, delta, temp, NULL)) {
    case OK:
        out_string(c, temp);
        break;
    case NON_NUMERIC:
        out_string(c, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        break;
    case EOM:
        out_string(c, "SERVER_ERROR out of memory");
        break;
    case DELTA_ITEM_NOT_FOUND:
        pthread_mutex_lock(&c->thread->stats.mutex);
        if (incr) {
            c->thread->stats.incr_misses++;
        } else {
            c->thread->stats.decr_misses++;
        }
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
        break;
    case DELTA_ITEM_CAS_MISMATCH:
        break; /* Should never get here */
    }
}

/*
 * adds a delta value to a numeric item.
 *
 * c     connection requesting the operation
 * it    item to adjust
 * incr  true to increment value, false to decrement
 * delta amount to adjust value by
 * buf   buffer for response string
 *
 * returns a response string to send back to the client.
 */
enum delta_result_type do_add_delta(conn *c, const char *key, const size_t nkey,
                                    const bool incr, const int64_t delta,
                                    char *buf, uint64_t *cas,
                                    const uint32_t hv) {
    char *ptr;
    uint64_t value;
    int res;
    item *it;

    it = do_item_get(key, nkey, hv);
    if (!it) {
        return DELTA_ITEM_NOT_FOUND;
    }

    if (cas != NULL && *cas != 0 && ITEM_get_cas(it) != *cas) {
        do_item_remove(it);
        return DELTA_ITEM_CAS_MISMATCH;
    }

    ptr = ITEM_data(it);

    if (!safe_strtoull(ptr, &value)) {
        do_item_remove(it);
        return NON_NUMERIC;
    }

    if (incr) {
        value += delta;
        MEMCACHED_COMMAND_INCR(c->sfd, ITEM_key(it), it->nkey, value);
    } else {
        if(delta > value) {
            value = 0;
        } else {
            value -= delta;
        }
        MEMCACHED_COMMAND_DECR(c->sfd, ITEM_key(it), it->nkey, value);
    }

    pthread_mutex_lock(&c->thread->stats.mutex);
    if (incr) {
        c->thread->stats.slab_stats[it->slabs_clsid].incr_hits++;
    } else {
        c->thread->stats.slab_stats[it->slabs_clsid].decr_hits++;
    }
    pthread_mutex_unlock(&c->thread->stats.mutex);

    snprintf(buf, INCR_MAX_STORAGE_LEN, "%llu", (unsigned long long)value);
    res = strlen(buf);
    if (res + 2 > it->nbytes || it->refcount != 1) { /* need to realloc */
        item *new_it;
        new_it = do_item_alloc(ITEM_key(it), it->nkey, atoi(ITEM_suffix(it) + 1), it->exptime, res + 2, hv);
        if (new_it == 0) {
            do_item_remove(it);
            return EOM;
        }
        memcpy(ITEM_data(new_it), buf, res);
        memcpy(ITEM_data(new_it) + res, "\r\n", 2);
        item_replace(it, new_it, hv);
        // Overwrite the older item's CAS with our new CAS since we're
        // returning the CAS of the old item below.
        ITEM_set_cas(it, (settings.use_cas) ? ITEM_get_cas(new_it) : 0);
        do_item_remove(new_it);       /* release our reference */
    } else { /* replace in-place */
        /* When changing the value without replacing the item, we
           need to update the CAS on the existing item. */
        mutex_lock(&cache_lock); /* FIXME */
        ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);
        mutex_unlock(&cache_lock);

        memcpy(ITEM_data(it), buf, res);
        memset(ITEM_data(it) + res, ' ', it->nbytes - res - 2);
        do_item_update(it);
    }

    if (cas) {
        *cas = ITEM_get_cas(it);    /* swap the incoming CAS value */
    }
    do_item_remove(it);         /* release our reference */
    return OK;
}

static void process_delete_command(conn *c, token_t *tokens, const size_t ntokens) {
    char *key;
    size_t nkey;
    item *it;

    assert(c != NULL);

    if (ntokens > 3) {
        bool hold_is_zero = strcmp(tokens[KEY_TOKEN+1].value, "0") == 0;
        bool sets_noreply = set_noreply_maybe(c, tokens, ntokens);
        bool valid = (ntokens == 4 && (hold_is_zero || sets_noreply))
            || (ntokens == 5 && hold_is_zero && sets_noreply);
        if (!valid) {
            out_string(c, "CLIENT_ERROR bad command line format.  "
                       "Usage: delete <key> [noreply]");
            return;
        }
    }


    key = tokens[KEY_TOKEN].value;
    nkey = tokens[KEY_TOKEN].length;

    if(nkey > KEY_MAX_LENGTH) {
        out_string(c, "CLIENT_ERROR bad command line format");
        return;
    }

    if (settings.detail_enabled) {
        stats_prefix_record_delete(key, nkey);
    }

    it = item_get(key, nkey);
    if (it) {
        MEMCACHED_COMMAND_DELETE(c->sfd, ITEM_key(it), it->nkey);

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.slab_stats[it->slabs_clsid].delete_hits++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        item_unlink(it);
        item_remove(it);      /* release our reference */
        out_string(c, "DELETED");
    } else {
        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.delete_misses++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        out_string(c, "NOT_FOUND");
    }
}

static void process_verbosity_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int level;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    level = strtoul(tokens[1].value, NULL, 10);
    settings.verbose = level > MAX_VERBOSITY_LEVEL ? MAX_VERBOSITY_LEVEL : level;
    out_string(c, "OK");
    return;
}

static void process_slabs_automove_command(conn *c, token_t *tokens, const size_t ntokens) {
    unsigned int level;

    assert(c != NULL);

    set_noreply_maybe(c, tokens, ntokens);

    level = strtoul(tokens[2].value, NULL, 10);
    if (level == 0) {
        settings.slab_automove = 0;
    } else if (level == 1 || level == 2) {
        settings.slab_automove = level;
    } else {
        out_string(c, "ERROR");
        return;
    }
    out_string(c, "OK");
    return;
}

static void process_command(conn *c, char *command) {

    token_t tokens[MAX_TOKENS];
    size_t ntokens;
    int comm;

    assert(c != NULL);

    MEMCACHED_PROCESS_COMMAND_START(c->sfd, c->rcurr, c->rbytes);

    if (settings.verbose > 1)
        fprintf(stderr, "<%d %s\n", c->sfd, command);

    /*
     * for commands set/add/replace, we build an item and read the data
     * directly into it, then continue in nread_complete().
     */

    c->msgcurr = 0;
    c->msgused = 0;
    c->iovused = 0;
    if (add_msghdr(c) != 0) {
        out_string(c, "SERVER_ERROR out of memory preparing response");
        return;
    }

    ntokens = tokenize_command(command, tokens, MAX_TOKENS);
    if (ntokens >= 3 &&
        ((strcmp(tokens[COMMAND_TOKEN].value, "get") == 0) ||
         (strcmp(tokens[COMMAND_TOKEN].value, "bget") == 0))) {

        process_get_command(c, tokens, ntokens, false);

    } else if ((ntokens == 6 || ntokens == 7) &&
               ((strcmp(tokens[COMMAND_TOKEN].value, "add") == 0 && (comm = NREAD_ADD)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "set") == 0 && (comm = NREAD_SET)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "replace") == 0 && (comm = NREAD_REPLACE)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "prepend") == 0 && (comm = NREAD_PREPEND)) ||
                (strcmp(tokens[COMMAND_TOKEN].value, "append") == 0 && (comm = NREAD_APPEND)) )) {

        process_update_command(c, tokens, ntokens, comm, false);

    } else if ((ntokens == 7 || ntokens == 8) && (strcmp(tokens[COMMAND_TOKEN].value, "cas") == 0 && (comm = NREAD_CAS))) {

        process_update_command(c, tokens, ntokens, comm, true);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "incr") == 0)) {

        process_arithmetic_command(c, tokens, ntokens, 1);

    } else if (ntokens >= 3 && (strcmp(tokens[COMMAND_TOKEN].value, "gets") == 0)) {

        process_get_command(c, tokens, ntokens, true);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "decr") == 0)) {

        process_arithmetic_command(c, tokens, ntokens, 0);

    } else if (ntokens >= 3 && ntokens <= 5 && (strcmp(tokens[COMMAND_TOKEN].value, "delete") == 0)) {

        process_delete_command(c, tokens, ntokens);

    } else if ((ntokens == 4 || ntokens == 5) && (strcmp(tokens[COMMAND_TOKEN].value, "touch") == 0)) {

        process_touch_command(c, tokens, ntokens);

    } else if (ntokens >= 2 && (strcmp(tokens[COMMAND_TOKEN].value, "stats") == 0)) {

        process_stat(c, tokens, ntokens);

    } else if (ntokens >= 2 && ntokens <= 4 && (strcmp(tokens[COMMAND_TOKEN].value, "flush_all") == 0)) {
        time_t exptime = 0;

        set_noreply_maybe(c, tokens, ntokens);

        pthread_mutex_lock(&c->thread->stats.mutex);
        c->thread->stats.flush_cmds++;
        pthread_mutex_unlock(&c->thread->stats.mutex);

        if(ntokens == (c->noreply ? 3 : 2)) {
            settings.oldest_live = current_time - 1;
            item_flush_expired();
            out_string(c, "OK");
            return;
        }

        exptime = strtol(tokens[1].value, NULL, 10);
        if(errno == ERANGE) {
            out_string(c, "CLIENT_ERROR bad command line format");
            return;
        }

        /*
          If exptime is zero realtime() would return zero too, and
          realtime(exptime) - 1 would overflow to the max unsigned
          value.  So we process exptime == 0 the same way we do when
          no delay is given at all.
        */
        if (exptime > 0)
            settings.oldest_live = realtime(exptime) - 1;
        else /* exptime == 0 */
            settings.oldest_live = current_time - 1;
        item_flush_expired();
        out_string(c, "OK");
        return;

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "version") == 0)) {

        out_string(c, "VERSION " VERSION);

    } else if (ntokens == 2 && (strcmp(tokens[COMMAND_TOKEN].value, "quit") == 0)) {

        conn_set_state(c, conn_closing);

    } else if (ntokens > 1 && strcmp(tokens[COMMAND_TOKEN].value, "slabs") == 0) {
        if (ntokens == 5 && strcmp(tokens[COMMAND_TOKEN + 1].value, "reassign") == 0) {
            int src, dst, rv;

            if (settings.slab_reassign == false) {
                out_string(c, "CLIENT_ERROR slab reassignment disabled");
                return;
            }

            src = strtol(tokens[2].value, NULL, 10);
            dst = strtol(tokens[3].value, NULL, 10);

            if (errno == ERANGE) {
                out_string(c, "CLIENT_ERROR bad command line format");
                return;
            }

            rv = slabs_reassign(src, dst);
            switch (rv) {
            case REASSIGN_OK:
                out_string(c, "OK");
                break;
            case REASSIGN_RUNNING:
                out_string(c, "BUSY currently processing reassign request");
                break;
            case REASSIGN_BADCLASS:
                out_string(c, "BADCLASS invalid src or dst class id");
                break;
            case REASSIGN_NOSPARE:
                out_string(c, "NOSPARE source class has no spare pages");
                break;
            case REASSIGN_SRC_DST_SAME:
                out_string(c, "SAME src and dst class are identical");
                break;
            }
            return;
        } else if (ntokens == 4 &&
            (strcmp(tokens[COMMAND_TOKEN + 1].value, "automove") == 0)) {
            process_slabs_automove_command(c, tokens, ntokens);
        } else {
            out_string(c, "ERROR");
        }
    } else if ((ntokens == 3 || ntokens == 4) && (strcmp(tokens[COMMAND_TOKEN].value, "verbosity") == 0)) {
        process_verbosity_command(c, tokens, ntokens);
    } else {
        out_string(c, "ERROR");
    }
    return;
}

/* 解析数据，memcached支持二进制协议和文本协议 */
static int try_read_command(conn *c)
{
    assert(c != NULL);
    assert(c->rcurr <= (c->rbuf + c->rsize));
    assert(c->rbytes > 0);
    if (c->protocol == negotiating_prot || c->transport == udp_transport)
    {
        //二进制协议有标志，按标志进行区分
        if ((unsigned char) c->rbuf[0] == (unsigned char) PROTOCOL_BINARY_REQ)
        {
            c->protocol = binary_prot;//二进制协议
        }
        else
        {
            c->protocol = ascii_prot;//文本协议
        }

        if (settings.verbose > 1)
        {
            fprintf(stderr, "%d: Client using the %s protocol\n", c->sfd,
                    prot_text(c->protocol));
        }
    }
    //如果是二进制协议
    if (c->protocol == binary_prot)
    {
        //二进制协议读取到的数据小于二进制协议的头部长度
        if (c->rbytes < sizeof(c->binary_header))
        {
            //返回继续读数据
            return 0;
        }
        else
        {
    #ifdef NEED_ALIGN
            //如果需要对齐，则按8字节对齐，对齐能提高CPU读取的效率
            if (((long)(c->rcurr)) % 8 != 0)
            {
                //从c->rcurr拷贝c->rbytes字节的数据到c->rbuf中
                memmove(c->rbuf, c->rcurr, c->rbytes);
                c->rcurr = c->rbuf;
                if (settings.verbose > 1)
                {
                    fprintf(stderr, "%d: Realign input buffer\n", c->sfd);
                }
            }
    #endif
            protocol_binary_request_header* req;//二进制协议头
            req = (protocol_binary_request_header*) c->rcurr;
            //调试信息
            if (settings.verbose > 1)
            {
                /* Dump the packet before we convert it to host order */
                int ii;
                fprintf(stderr, "<%d Read binary protocol data:", c->sfd);
                for (ii = 0; ii < sizeof(req->bytes); ++ii)
                {
                    if (ii % 4 == 0)
                    {
                        fprintf(stderr, "\n<%d   ", c->sfd);
                    }
                    fprintf(stderr, " 0x%02x", req->bytes[ii]);
                }
                fprintf(stderr, "\n");
            }
            c->binary_header = *req;
            c->binary_header.request.keylen = ntohs(req->request.keylen);//二进制协议相关内容
            c->binary_header.request.bodylen = ntohl(req->request.bodylen);
            c->binary_header.request.cas = ntohll(req->request.cas);
            //判断魔数是否合法，魔数用来防止TCP粘包
            if (c->binary_header.request.magic != PROTOCOL_BINARY_REQ)
            {
                if (settings.verbose)
                {
                    fprintf(stderr, "Invalid magic:  %x\n",
                            c->binary_header.request.magic);
                }
                conn_set_state(c, conn_closing);
                return -1;
            }
            c->msgcurr = 0;
			c->msgused = 0;
			c->iovused = 0;
			if (add_msghdr(c) != 0)
			{
				out_string(c, "SERVER_ERROR out of memory");
				return 0;
			}
			c->cmd = c->binary_header.request.opcode;
			c->keylen = c->binary_header.request.keylen;
			c->opaque = c->binary_header.request.opaque;
			//清除客户端传递的cas值
			c->cas = 0;
			dispatch_bin_command(c);//协议数据处理
			c->rbytes -= sizeof(c->binary_header);//更新已经读取到的字节数据
			c->rcurr += sizeof(c->binary_header);//更新缓冲区的路标信息
        }
    }
    //文本协议
    else
    {
        char *el, *cont;
        if (c->rbytes == 0) return 0;
        el = memchr(c->rcurr, '\n', c->rbytes);
        if (!el)
        {
            if (c->rbytes > 1024) {
                /*
                 * We didn't have a '\n' in the first k. This _has_ to be a
                 * large multiget, if not we should just nuke the connection.
                 */
                char *ptr = c->rcurr;
                while (*ptr == ' ') { /* ignore leading whitespaces */
                    ++ptr;
                }

                if (ptr - c->rcurr > 100 ||
                    (strncmp(ptr, "get ", 4) && strncmp(ptr, "gets ", 5))) {
                    conn_set_state(c, conn_closing);
                    return 1;
                }
            }
            return 0;
        }
        cont = el + 1;
        if ((el - c->rcurr) > 1 && *(el - 1) == '\r') {
            el--;
        }
        *el = '\0';
        assert(cont <= (c->rcurr + c->rbytes));
        process_command(c, c->rcurr);
        c->rbytes -= (cont - c->rcurr);
        c->rcurr = cont;
        assert(c->rcurr <= (c->rbuf + c->rsize));
    }
    return 1;
}

/*
 * read a UDP request.
 */
static enum try_read_result try_read_udp(conn *c)
{
	int res;

	assert(c != NULL);

	c->request_addr_size = sizeof(c->request_addr);
	res = recvfrom(c->sfd, c->rbuf, c->rsize, 0, &c->request_addr,
			&c->request_addr_size);//执行UDP的网络读取
	if (res > 8)//UDP数据包大小大于8，已经有可能是业务数据包
	{
		unsigned char *buf = (unsigned char *) c->rbuf;
		pthread_mutex_lock(&c->thread->stats.mutex);
		c->thread->stats.bytes_read += res;//更新每个线程的统计数据
		pthread_mutex_unlock(&c->thread->stats.mutex);

		/* Beginning of UDP packet is the request ID; save it. */
		c->request_id = buf[0] * 256 + buf[1];//UDP为了防止丢包，增加了确认字段

		/* If this is a multi-packet request, drop it. */
		if (buf[4] != 0 || buf[5] != 1)//一些业务的特征信息判断
		{
			out_string(c, "SERVER_ERROR multi-packet request not supported");
			return READ_NO_DATA_RECEIVED;
		}

		/* Don't care about any of the rest of the header. */
		res -= 8;
		memmove(c->rbuf, c->rbuf + 8, res);//调整缓冲区

		c->rbytes = res;//更新信息
		c->rcurr = c->rbuf;
		return READ_DATA_RECEIVED;
	}
	return READ_NO_DATA_RECEIVED;
}
/*
 * read from network as much as we can, handle buffer overflow and connection close. before reading, move the remaining
 * incomplete fragment of a command (if any) to the beginning of the buffer.
 *
 * To protect us from someone flooding a connection with bogus(伪造的) data causing the connection to eat up all available
 *  memory,break out and start looking at the data I've got after a number of reallocs...
 *
 * @return enum try_read_result
 */
static enum try_read_result try_read_network(conn *c)
{
	enum try_read_result gotdata = READ_NO_DATA_RECEIVED;
	int res;
	int num_allocs = 0;
	assert(c != NULL);
	//rcurr标记读缓冲区的开始位置，如果不在，通过memmove调整
	if (c->rcurr != c->rbuf)
	{
		if (c->rbytes != 0) memmove(c->rbuf, c->rcurr, c->rbytes);
		c->rcurr = c->rbuf;//rcurr指向读缓冲区起始位置
	}
	while (1)//循环读取
	{
		if (c->rbytes >= c->rsize)//已经读取到的数据大于读缓冲区的大小
		{
			if (num_allocs == 4)
			{
				return gotdata;
			}
			++num_allocs;
			char *new_rbuf = realloc(c->rbuf, c->rsize * 2);//按2倍扩容空间
			if (!new_rbuf)//realloc发生错误，也就是申请内存失败
			{
				if (settings.verbose > 0)fprintf(stderr, "Couldn't realloc input buffer\n");
				c->rbytes = 0; //忽略已经读取到的数据
				out_string(c, "SERVER_ERROR out of memory reading request");
				c->write_and_go = conn_closing;//下一个状态就是conn_closing状态
				return READ_MEMORY_ERROR;//返回错误
			}
			c->rcurr = c->rbuf = new_rbuf;//读缓冲区指向新的缓冲区
			c->rsize *= 2;//读缓冲区的大小扩大2倍
		}
		int avail = c->rsize - c->rbytes;//读缓冲区剩余空间
		res = read(c->sfd, c->rbuf + c->rbytes, avail);//执行网络读取，这个是非阻塞的读
		if (res > 0)//如果读取到了数据
		{
			pthread_mutex_lock(&c->thread->stats.mutex);
			c->thread->stats.bytes_read += res;//更新线程的统计数据
			pthread_mutex_unlock(&c->thread->stats.mutex);
			gotdata = READ_DATA_RECEIVED;//返回读取到数据的状态
			c->rbytes += res;//读取到的数据个数增加res
			if (res == avail)//最多读取到avail个，如果已经读到了，则可以尝试继续读取
			{
				continue;
			}
			else//否则，小于avail,表示已经没数据了，退出循环。
			{
				break;
			}
		}
		if (res == 0)//表示客户端关闭了连接
		{
			return READ_ERROR;
		}
		if (res == -1)//因为是非阻塞的，所以会返回下面的两个错误码
		{
			if (errno == EAGAIN || errno == EWOULDBLOCK)
			{
				break;
			}
			return READ_ERROR;//如果返回为负数，且不是上面两个数，则表示发生了其他错误，返回READ_ERROR
		}
	}
	return gotdata;
}

//更新libevent状态，也就是删除libevent事件后，重新注册libevent事件
static bool update_event(conn *c, const int new_flags)
{
	assert(c != NULL);
	struct event_base *base = c->event.ev_base;
	if (c->ev_flags == new_flags)return true;
	if (event_del(&c->event) == -1)return false;//删除旧的事件
	event_set(&c->event, c->sfd, new_flags, event_handler, (void *) c);//注册新事件
	event_base_set(base, &c->event);
	c->ev_flags = new_flags;
	if (event_add(&c->event, 0) == -1)return false;
	return true;
}

/*
 * Sets whether we are listening for new connections or not.
 */
void do_accept_new_conns(const bool do_accept) {
    conn *next;

    for (next = listen_conn; next; next = next->next) {
        if (do_accept) {
            update_event(next, EV_READ | EV_PERSIST);
            if (listen(next->sfd, settings.backlog) != 0) {
                perror("listen");
            }
        }
        else {
            update_event(next, 0);
            if (listen(next->sfd, 0) != 0) {
                perror("listen");
            }
        }
    }

    if (do_accept) {
        STATS_LOCK();
        stats.accepting_conns = true;
        STATS_UNLOCK();
    } else {
        STATS_LOCK();
        stats.accepting_conns = false;
        stats.listen_disabled_num++;
        STATS_UNLOCK();
        allow_new_conns = false;
        maxconns_handler(-42, 0, 0);
    }
}

/*
 * Transmit the next chunk of data from our list of msgbuf structures.
 *
 * Returns:
 *   TRANSMIT_COMPLETE   All done writing.
 *   TRANSMIT_INCOMPLETE More data remaining to write.
 *   TRANSMIT_SOFT_ERROR Can't write any more right now.
 *   TRANSMIT_HARD_ERROR Can't write (c->state is set to conn_closing)
 */
static enum transmit_result transmit(conn *c)
{
    assert(c != NULL);

    if (c->msgcurr < c->msgused && c->msglist[c->msgcurr].msg_iovlen == 0)
    {
        /* Finished writing the current msg; advance to the next. */
        c->msgcurr++;
    }
    if (c->msgcurr < c->msgused)
    {
        ssize_t res;
        struct msghdr *m = &c->msglist[c->msgcurr];

        res = sendmsg(c->sfd, m, 0);
        if (res > 0) {
            pthread_mutex_lock(&c->thread->stats.mutex);
            c->thread->stats.bytes_written += res;
            pthread_mutex_unlock(&c->thread->stats.mutex);

            /* We've written some of the data. Remove the completed
               iovec entries from the list of pending writes. */
            while (m->msg_iovlen > 0 && res >= m->msg_iov->iov_len)
            {
                res -= m->msg_iov->iov_len;
                m->msg_iovlen--;
                m->msg_iov++;
            }

            /* Might have written just part of the last iovec entry;
               adjust it so the next write will do the rest. */
            if (res > 0)
            {
                m->msg_iov->iov_base = (caddr_t)m->msg_iov->iov_base + res;
                m->msg_iov->iov_len -= res;
            }
            return TRANSMIT_INCOMPLETE;
        }
        if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            if (!update_event(c, EV_WRITE | EV_PERSIST)) {
                if (settings.verbose > 0)
                    fprintf(stderr, "Couldn't update event\n");
                conn_set_state(c, conn_closing);
                return TRANSMIT_HARD_ERROR;
            }
            return TRANSMIT_SOFT_ERROR;
        }
        /* if res == 0 or res == -1 and error is not EAGAIN or EWOULDBLOCK,
           we have a real error, on which we close the connection */
        if (settings.verbose > 0)
            perror("Failed to write, and not due to blocking");

        if (IS_UDP(c->transport))
            conn_set_state(c, conn_read);
        else
            conn_set_state(c, conn_closing);
        return TRANSMIT_HARD_ERROR;
    }
    else
    {
        return TRANSMIT_COMPLETE;
    }
}

//整个memcached的服务处理状态机
static void drive_machine(conn *c)
{
    bool stop = false;
    int sfd, flags = 1;
    socklen_t addrlen;
    struct sockaddr_storage addr;
    int nreqs = settings.reqs_per_event;
    int res;
    const char *str;
    assert(c != NULL);
    while (!stop)
    {
        switch(c->state)
        {
        case conn_listening://监听状态，只有主线程会进入该状态，而且，非错误状态下，主线程一直处于这个状态
            addrlen = sizeof(addr);
            //Master线程(main)进入状态机之后执行accept操作，这个操作也是非阻塞的
            if ((sfd = accept(c->sfd, (struct sockaddr *)&addr, &addrlen)) == -1)
            {
            	//非阻塞模型，这个错误码继续等待
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    /* these are transient, so don't log anything */
                    stop = true;
                }
                //连接超载
                else if (errno == EMFILE)
                {
                    if (settings.verbose > 0) fprintf(stderr, "Too many open connections\n");
                    accept_new_conns(false);
                    stop = true;
                }
                else
                {
                    perror("accept()");
                    stop = true;
                }
                break;
            }
            //已经accept成功，将accept之后的描述符设置为非阻塞的
            if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0)
            {
                perror("setting O_NONBLOCK");
                close(sfd);
                break;
            }
            //判断是否超过最大连接数
            if (settings.maxconns_fast && stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1)
            {
                str = "ERROR Too many open connections\r\n";
                res = write(sfd, str, strlen(str));
                close(sfd);
                STATS_LOCK();
                stats.rejected_conns++;
                STATS_UNLOCK();
            }
            else
            {
            	//将连接分发给某个work进程，设置连接状态为conn_new_cmd
                dispatch_conn_new(sfd, conn_new_cmd, EV_READ | EV_PERSIST, DATA_BUFFER_SIZE, tcp_transport);
            }
            stop = true;
            break;

        case conn_waiting://等待数据状态
        	if (!update_event(c, EV_READ | EV_PERSIST))//修改libevent状态，读取数据
			{
				if (settings.verbose > 0)
					fprintf(stderr, "Couldn't update event\n");
				conn_set_state(c, conn_closing);
				break;
			}
			conn_set_state(c, conn_read);//进入读数据状态
			stop = true;
            break;

        case conn_read://从网络中读取数据状态
            res = IS_UDP(c->transport) ? try_read_udp(c) : try_read_network(c);//判断采用UDP协议还是TCP协议
            switch (res)
            {
            case READ_NO_DATA_RECEIVED://未读取到数据
                conn_set_state(c, conn_waiting);//继续等待
                break;
            case READ_DATA_RECEIVED://读取到数据
                conn_set_state(c, conn_parse_cmd);//开始解析数据
                break;
            case READ_ERROR://读取发生错误
                conn_set_state(c, conn_closing);//关闭连接
                break;
            case READ_MEMORY_ERROR: /* 申请内存空间错误，继续尝试 */
                /* State already set by try_read_network */
                break;
            }
            break;

         /* 按照协议解析读取到的网络数据，判断具体的指令，如果是update的指令，那么需要跳转到conn_nread中，因为需要在从网络中读取固定byte的数据，
         如果是查询之类的指令，就直接查询完成后，跳转到conn_mwrite中，返回数据 */
        case conn_parse_cmd :
        	//如果读取到的数据不够，我们继续等待，等读取到的数据够了，再进行解析
            if (try_read_command(c) == 0)
            {
                conn_set_state(c, conn_waiting);
            }
            break;

		/* Only process nreqs at a time to avoid starving(饿死) other connections。主线程触发accept事件后,就将其分发到worker
		线程中,注意这个时候链接的状态变成了conn_new_cmd。worker线程触发时间后回调thread_libevent_process函数,该函数读出一个字节后,从
		该线程队列里取出一个cq_item,调用conn_new函数创建conn对象.从而进入conn_new_cmd处理逻辑。在reset_cmd_handler中进行判断，假如
		发现连接符中有数据则跳转到conn_parse_cmd.否则处于进入conn_waiting状态 */
        case conn_new_cmd:
            --nreqs;//全局变量，记录每个libevent实例处理的事件，通过初始启动参数配置，用于负载平衡
            //还可以处理请求
            if (nreqs >= 0)
            {
                reset_cmd_handler(c);//整理缓冲区
            }
            //拒绝请求
            else
            {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.conn_yields++;//更新统计数据
                pthread_mutex_unlock(&c->thread->stats.mutex);
                //如果缓冲区有数据，则需要处理
                if (c->rbytes > 0)
                {
                    /* We have already read in data into the input buffer,
                       so libevent will most likely not signal read events
                       on the socket (unless more data is available. As a
                       hack we should just put in a request to write data,
                       because that should be possible ;-)
                    */
                	//更新libevent状态
                    if (!update_event(c, EV_WRITE | EV_PERSIST))
                    {
                        if (settings.verbose > 0) fprintf(stderr, "Couldn't update event\n");
                        conn_set_state(c, conn_closing);//关闭连接
                    }
                }
                stop = true;
            }
            break;

        //解析完一些数据之后，会进入到conn_nread的流程，也就是读取指定数目数据的过程，这个过程主要是做具体的操作了，比如get，add，set操作
        case conn_nread:
            if (c->rlbytes == 0)
            {
                complete_nread(c);//调用get等命令的处理函数
                break;
            }
            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0)
            {
                int tocopy = c->rbytes > c->rlbytes ? c->rlbytes : c->rbytes;
                if (c->ritem != c->rcurr) {
                    memmove(c->ritem, c->rcurr, tocopy);
                }
                c->ritem += tocopy;
                c->rlbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                if (c->rlbytes == 0) {
                    break;
                }
            }
            /*  now try reading from the socket */
            res = read(c->sfd, c->ritem, c->rlbytes);
            if (res > 0)
            {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                if (c->rcurr == c->ritem) {
                    c->rcurr += res;
                }
                c->ritem += res;
                c->rlbytes -= res;
                break;
            }
            if (res == 0)
            { /* end of stream */
                conn_set_state(c, conn_closing);
                break;
            }
            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0)
            {
                fprintf(stderr, "Failed to read, and not due to blocking:\n"
                        "errno: %d %s \n"
                        "rcurr=%lx ritem=%lx rbuf=%lx rlbytes=%d rsize=%d\n",
                        errno, strerror(errno),
                        (long)c->rcurr, (long)c->ritem, (long)c->rbuf,
                        (int)c->rlbytes, (int)c->rsize);
            }
            conn_set_state(c, conn_closing);
            break;

        case conn_swallow:
            /* we are reading sbytes and throwing them away */
            if (c->sbytes == 0) {
                conn_set_state(c, conn_new_cmd);
                break;
            }
            /* first check if we have leftovers in the conn_read buffer */
            if (c->rbytes > 0) {
                int tocopy = c->rbytes > c->sbytes ? c->sbytes : c->rbytes;
                c->sbytes -= tocopy;
                c->rcurr += tocopy;
                c->rbytes -= tocopy;
                break;
            }
            /*  now try reading from the socket */
            res = read(c->sfd, c->rbuf, c->rsize > c->sbytes ? c->sbytes : c->rsize);
            if (res > 0)
            {
                pthread_mutex_lock(&c->thread->stats.mutex);
                c->thread->stats.bytes_read += res;
                pthread_mutex_unlock(&c->thread->stats.mutex);
                c->sbytes -= res;
                break;
            }
            if (res == 0) { /* end of stream */
                conn_set_state(c, conn_closing);
                break;
            }
            if (res == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                if (!update_event(c, EV_READ | EV_PERSIST)) {
                    if (settings.verbose > 0)fprintf(stderr, "Couldn't update event\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
                stop = true;
                break;
            }
            /* otherwise we have a real error, on which we close the connection */
            if (settings.verbose > 0)
                fprintf(stderr, "Failed to read, and not due to blocking\n");
            conn_set_state(c, conn_closing);
            break;

        case conn_write:
            /*
             * We want to write out a simple response. If we haven't already,
             * assemble it into a msgbuf list (this will be a single-entry
             * list for TCP or a two-entry list for UDP).
             */
            if (c->iovused == 0 || (IS_UDP(c->transport) && c->iovused == 1))
            {
                if (add_iov(c, c->wcurr, c->wbytes) != 0)
                {
                    if (settings.verbose > 0)fprintf(stderr, "Couldn't build response\n");
                    conn_set_state(c, conn_closing);
                    break;
                }
            }
            /* fall through... */
        case conn_mwrite://发送get等查询命令的数据给客户端
          if (IS_UDP(c->transport) && c->msgcurr == 0 && build_udp_headers(c) != 0)
          {
            if (settings.verbose > 0) fprintf(stderr, "Failed to build UDP headers\n");
            conn_set_state(c, conn_closing);
            break;
          }
            switch (transmit(c)) //发送数据
            {
            case TRANSMIT_COMPLETE:
                if (c->state == conn_mwrite)
                {
                    while (c->ileft > 0)
                    {
                        item *it = *(c->icurr);
                        assert((it->it_flags & ITEM_SLABBED) == 0);
                        item_remove(it);
                        c->icurr++;
                        c->ileft--;
                    }
                    while (c->suffixleft > 0)
                    {
                        char *suffix = *(c->suffixcurr);
                        cache_free(c->thread->suffix_cache, suffix);
                        c->suffixcurr++;
                        c->suffixleft--;
                    }
                    /* XXX:  I don't know why this wasn't the general case */
                    if(c->protocol == binary_prot)
                    {
                        conn_set_state(c, c->write_and_go);
                    }
                    else
                    {
                        conn_set_state(c, conn_new_cmd);
                    }
                }
                else if (c->state == conn_write)
                {
                    if (c->write_and_free)
                    {
                        free(c->write_and_free);
                        c->write_and_free = 0;
                    }
                    conn_set_state(c, c->write_and_go);
                }
                else
                {
                    if (settings.verbose > 0)
                        fprintf(stderr, "Unexpected state %d\n", c->state);
                    conn_set_state(c, conn_closing);
                }
                break;

            case TRANSMIT_INCOMPLETE:
            case TRANSMIT_HARD_ERROR:
                break;                   /* Continue in state machine. */

            case TRANSMIT_SOFT_ERROR:
                stop = true;
                break;
            }
            break;

        case conn_closing:
            if (IS_UDP(c->transport))
                conn_cleanup(c);
            else
                conn_close(c);
            stop = true;
            break;

        case conn_max_state:
            assert(false);
            break;
        }
    }
    return;
}

//libevent事件回调函数的处理，回调函数被调用时，表明Memcached监听的端口号有网络事件到了
void event_handler(const int fd, const short which, void *arg)
{
    conn *c;
    c = (conn *)arg;
    assert(c != NULL);
    c->which = which;
    /* sanity */
    if (fd != c->sfd)
    {
        if (settings.verbose > 0)fprintf(stderr, "Catastrophic: event fd doesn't match conn fd!\n");
        conn_close(c);
        return;
    }
    //进入业务处理状态机
    drive_machine(c);
    /* wait for next event */
    return;
}

//建立socket
static int new_socket(struct addrinfo *ai)
{
    int sfd;
    int flags;

    //调用系统函数建立socket
    if ((sfd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol)) == -1) {
        return -1;
    }

    //设定socket为非阻塞的
    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}


/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 * 如果是UDP协议，调整发送缓存到最大值
 */
static void maximize_sndbuf(const int sfd) {
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    /* 读取socket的选项，SO_SNDBF表示发送缓存. */
    if (getsockopt(sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
        if (settings.verbose > 0)
            perror("getsockopt(SO_SNDBUF)");
        return;
    }

    /* Binary-search for the real maximum. */
    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(sfd, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }

    if (settings.verbose > 1)
        fprintf(stderr, "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
}

/**
 * Create a socket and bind it to a specific port number
 * @param interface the interface to bind to
 * @param port the port number to bind to
 * @param transport the transport protocol (TCP / UDP)
 * @param portnumber_file A filepointer to write the port numbers to
 *        when they are successfully added to the list of ports we
 *        listen on.
 * 执行真正的绑定
 */
static int server_socket(const char *interface,int port, enum network_transport transport, FILE *portnumber_file) {
    int sfd;
    struct linger ling = {0, 0};
    struct addrinfo *ai;
    struct addrinfo *next;
    //设定协议无关，用于监听的标志位
    struct addrinfo hints = { .ai_flags = AI_PASSIVE,.ai_family = AF_UNSPEC };
    char port_buf[NI_MAXSERV];
    int error;
    int success = 0;
    int flags =1;
    //指定socket的类型，如果是udp，则用数据报协议，如果是tcp,则用数据流协议
    hints.ai_socktype = IS_UDP(transport) ? SOCK_DGRAM : SOCK_STREAM;
    if (port == -1) {
        port = 0;
    }
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    //调用getaddrinfo,将主机地址和端口号映射成为socket地址信息，地址信息由ai带回
    error= getaddrinfo(interface, port_buf, &hints, &ai);
    if (error != 0)
    {
        if (error != EAI_SYSTEM)
          fprintf(stderr, "getaddrinfo(): %s\n", gai_strerror(error));
        else
          perror("getaddrinfo()");
        return 1;
    }
    /*getaddrinfo返回多个addrinfo的情形有如下两种：1.如果与interface参数关联的地址有多个，那么适用于所请求地址簇的每个地址都返回一个对应的结构。
    2.如果port_buf参数指定的服务支持多个套接口类型，那么每个套接口类型都可能返回一个对应的结构。 */
    for (next= ai; next; next= next->ai_next)
    {
        conn *listen_conn_add;
        //为每个地址信息建立socket
        if ((sfd = new_socket(next)) == -1)
        {
            /*建立socket过程中可能发生的，比如打开文件描述符过多等*/
            if (errno == EMFILE) {
                /* ...unless we're out of fds */
                perror("server_socket");
                exit(EX_OSERR);
            }
            continue;
        }
#ifdef IPV6_V6ONLY
        if (next->ai_family == AF_INET6) {
        	//设定IPV6的选项值，设置了IPV6_V6ONLY，表示只收发IPV6的数据包，此时IPV4和IPV6可以绑定到同一个端口而不影响数据的收发
            error = setsockopt(sfd, IPPROTO_IPV6, IPV6_V6ONLY, (char *) &flags, sizeof(flags));
            if (error != 0) {
                perror("setsockopt");
                close(sfd);
                continue;
            }
        }
#endif
        //设定socket选项，SO_REUSEADDR表示重用地址信息
        setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
        //如果是UDP协议
        if (IS_UDP(transport))
        {
            maximize_sndbuf(sfd);//扩大发送缓冲区
        }
        else
        {
        	//设定socket选项，SO_KEEPALIVE表示保活
            error = setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
            if (error != 0) perror("setsockopt");
            //设定socket选项，SO_LINGER表示执行close操作时，如果缓冲区还有数据，可以继续发送
            error = setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));
            if (error != 0) perror("setsockopt");
            //设定IP选项，TCP_NODELAY表示禁用Nagle算法
            error = setsockopt(sfd, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));
            if (error != 0) perror("setsockopt");
        }
        //执行绑定操作
        if (bind(sfd, next->ai_addr, next->ai_addrlen) == -1)
        {
            if (errno != EADDRINUSE)
            {
                perror("bind()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            close(sfd);
            continue;
        }
        else
        {
            success++;
            //如果不是UDP协议，则执行监听操作，监听队列为初始启动的值
            if (!IS_UDP(transport) && listen(sfd, settings.backlog) == -1)
            {
                perror("listen()");
                close(sfd);
                freeaddrinfo(ai);
                return 1;
            }
            if (portnumber_file != NULL &&(next->ai_addr->sa_family == AF_INET || next->ai_addr->sa_family == AF_INET6))
            {
                union {
                    struct sockaddr_in in;
                    struct sockaddr_in6 in6;
                } my_sockaddr;
                socklen_t len = sizeof(my_sockaddr);
                //这时还没连接建立，调用getsockname不知道有什么用？
                if (getsockname(sfd, (struct sockaddr*)&my_sockaddr, &len)==0)
                {
                    if (next->ai_addr->sa_family == AF_INET)
                    {
                        fprintf(portnumber_file, "%s INET: %u\n", IS_UDP(transport) ? "UDP" : "TCP", ntohs(my_sockaddr.in.sin_port));
                    }
                    else
                    {
                        fprintf(portnumber_file, "%s INET6: %u\n",IS_UDP(transport) ? "UDP" : "TCP",ntohs(my_sockaddr.in6.sin6_port));
                    }
                }
            }
        }
        if (IS_UDP(transport))
        {
            int c;
            for (c = 0; c < settings.num_threads_per_udp; c++)
            {
                /* 分发连接，因为UDP没有连接建立的过程，直接进行连接的分发   */
                dispatch_conn_new(sfd, conn_read, EV_READ | EV_PERSIST,UDP_READ_BUFFER_SIZE, transport);
            }
        }
        else//TCP,建立连接
        {
            if (!(listen_conn_add = conn_new(sfd, conn_listening, EV_READ | EV_PERSIST, 1, transport, main_base)))
            {
                fprintf(stderr, "failed to create listening connection\n");
                exit(EXIT_FAILURE);
            }
            //建立的连接组成链表
            listen_conn_add->next = listen_conn;
            listen_conn = listen_conn_add;
        }
    }
    //释放资源
    freeaddrinfo(ai);
    /* Return zero iff we detected no errors in starting up connections */
    return success == 0;
}

//TCP和UDP使用的是同一个接口来创建监听和绑定
static int server_sockets(int port, enum network_transport transport,FILE *portnumber_file)
{
	//settings.inter指定的是要绑定的ip地址信息，如果为空，则表示是绑定本机一个ip
    if (settings.inter == NULL)
    {
    	//执行监听和绑定操作
        return server_socket(settings.inter, port, transport, portnumber_file);
    }
    //如果服务器有多个ip信息，可以在每个(ip,port)上面绑定一个Memcached实例，下面是一些输入参数的解析，解析完毕之后，执行绑定
    else
    {
        // tokenize them and bind to each one of them..
        char *b;
        int ret = 0;
        char *list = strdup(settings.inter);
        if (list == NULL) {
            fprintf(stderr, "Failed to allocate memory for parsing server interface string\n");
            return 1;
        }
        for (char *p = strtok_r(list, ";,", &b); p != NULL; p = strtok_r(NULL, ";,", &b))
        {
            int the_port = port;
            char *s = strchr(p, ':');
            if (s != NULL) {
                *s = '\0';
                ++s;
                if (!safe_strtol(s, &the_port)) {
                    fprintf(stderr, "Invalid port number: \"%s\"", s);
                    return 1;
                }
            }
            if (strcmp(p, "*") == 0) {
                p = NULL;
            }
            //绑定多次,循环调用单个的绑定函数
            ret |= server_socket(p, the_port, transport, portnumber_file);
        }
        free(list);
        return ret;
    }
}

static int new_socket_unix(void) {
    int sfd;
    int flags;

    if ((sfd = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket()");
        return -1;
    }

    if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 ||
        fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("setting O_NONBLOCK");
        close(sfd);
        return -1;
    }
    return sfd;
}

static int server_socket_unix(const char *path, int access_mask) {
    int sfd;
    struct linger ling = {0, 0};
    struct sockaddr_un addr;
    struct stat tstat;
    int flags =1;
    int old_umask;

    if (!path) {
        return 1;
    }

    if ((sfd = new_socket_unix()) == -1) {
        return 1;
    }

    /*
     * Clean up a previous socket file if we left it around
     */
    if (lstat(path, &tstat) == 0) {
        if (S_ISSOCK(tstat.st_mode))
            unlink(path);
    }

    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));
    setsockopt(sfd, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

    /*
     * the memset call clears nonstandard fields in some impementations
     * that otherwise mess things up.
     */
    memset(&addr, 0, sizeof(addr));

    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
    assert(strcmp(addr.sun_path, path) == 0);
    old_umask = umask( ~(access_mask&0777));
    if (bind(sfd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
        perror("bind()");
        close(sfd);
        umask(old_umask);
        return 1;
    }
    umask(old_umask);
    if (listen(sfd, settings.backlog) == -1) {
        perror("listen()");
        close(sfd);
        return 1;
    }
    if (!(listen_conn = conn_new(sfd, conn_listening,
                                 EV_READ | EV_PERSIST, 1,
                                 local_transport, main_base))) {
        fprintf(stderr, "failed to create listening connection\n");
        exit(EXIT_FAILURE);
    }

    return 0;
}

/*
 * We keep the current time of day in a global variable that's updated by a
 * timer event. This saves us a bunch of time() system calls (we really only
 * need to get the time once a second, whereas there can be tens of thousands
 * of requests a second) and allows us to use server-start-relative timestamps
 * rather than absolute UNIX timestamps, a space savings on systems where
 * sizeof(time_t) > sizeof(unsigned int).
 */
volatile rel_time_t current_time;
static struct event clockevent;

/* libevent uses a monotonic clock when available for event scheduling. Aside
 * from jitter, simply ticking our internal timer here is accurate enough.
 * Note that users who are setting explicit dates for expiration times *must*
 * ensure their clocks are correct before starting memcached. */
static void clock_handler(const int fd, const short which, void *arg) {
    struct timeval t = {.tv_sec = 1, .tv_usec = 0};
    static bool initialized = false;
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    static bool monotonic = false;
    static time_t monotonic_start;
#endif

    if (initialized) {
        /* only delete the event if it's actually there. */
        evtimer_del(&clockevent);
    } else {
        initialized = true;
        /* process_started is initialized to time() - 2. We initialize to 1 so
         * flush_all won't underflow during tests. */
#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
            monotonic = true;
            monotonic_start = ts.tv_sec - 2;
        }
#endif
    }

    evtimer_set(&clockevent, clock_handler, 0);
    event_base_set(main_base, &clockevent);
    evtimer_add(&clockevent, &t);

#if defined(HAVE_CLOCK_GETTIME) && defined(CLOCK_MONOTONIC)
    if (monotonic) {
        struct timespec ts;
        if (clock_gettime(CLOCK_MONOTONIC, &ts) == -1)
            return;
        current_time = (rel_time_t) (ts.tv_sec - monotonic_start);
        return;
    }
#endif
    {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        current_time = (rel_time_t) (tv.tv_sec - process_started);
    }
}

static void usage(void) {
    printf(PACKAGE " " VERSION "\n");
    printf("-p <num>      TCP port number to listen on (default: 11211)\n"
           "-U <num>      UDP port number to listen on (default: 11211, 0 is off)\n"
           "-s <file>     UNIX socket path to listen on (disables network support)\n"
           "-a <mask>     access mask for UNIX socket, in octal (default: 0700)\n"
           "-l <addr>     interface to listen on (default: INADDR_ANY, all addresses)\n"
           "              <addr> may be specified as host:port. If you don't specify\n"
           "              a port number, the value you specified with -p or -U is\n"
           "              used. You may specify multiple addresses separated by comma\n"
           "              or by using -l multiple times\n"

           "-d            run as a daemon\n"
           "-r            maximize core file limit\n"
           "-u <username> assume identity of <username> (only when run as root)\n"
           "-m <num>      max memory to use for items in megabytes (default: 64 MB)\n"
           "-M            return error on memory exhausted (rather than removing items)\n"
           "-c <num>      max simultaneous connections (default: 1024)\n"
           "-k            lock down all paged memory.  Note that there is a\n"
           "              limit on how much memory you may lock.  Trying to\n"
           "              allocate more than that would fail, so be sure you\n"
           "              set the limit correctly for the user you started\n"
           "              the daemon with (not for -u <username> user;\n"
           "              under sh this is done with 'ulimit -S -l NUM_KB').\n"
           "-v            verbose (print errors/warnings while in event loop)\n"
           "-vv           very verbose (also print client commands/reponses)\n"
           "-vvv          extremely verbose (also print internal state transitions)\n"
           "-h            print this help and exit\n"
           "-i            print memcached and libevent license\n"
           "-P <file>     save PID in <file>, only used with -d option\n"
           "-f <factor>   chunk size growth factor (default: 1.25)\n"
           "-n <bytes>    minimum space allocated for key+value+flags (default: 48)\n");
    printf("-L            Try to use large memory pages (if available). Increasing\n"
           "              the memory page size could reduce the number of TLB misses\n"
           "              and improve the performance. In order to get large pages\n"
           "              from the OS, memcached will allocate the total item-cache\n"
           "              in one large chunk.\n");
    printf("-D <char>     Use <char> as the delimiter between key prefixes and IDs.\n"
           "              This is used for per-prefix stats reporting. The default is\n"
           "              \":\" (colon). If this option is specified, stats collection\n"
           "              is turned on automatically; if not, then it may be turned on\n"
           "              by sending the \"stats detail on\" command to the server.\n");
    printf("-t <num>      number of threads to use (default: 4)\n");
    printf("-R            Maximum number of requests per event, limits the number of\n"
           "              requests process for a given connection to prevent \n"
           "              starvation (default: 20)\n");
    printf("-C            Disable use of CAS\n");
    printf("-b            Set the backlog queue limit (default: 1024)\n");
    printf("-B            Binding protocol - one of ascii, binary, or auto (default)\n");
    printf("-I            Override the size of each slab page. Adjusts max item size\n"
           "              (default: 1mb, min: 1k, max: 128m)\n");
#ifdef ENABLE_SASL
    printf("-S            Turn on Sasl authentication\n");
#endif
    printf("-o            Comma separated list of extended or experimental options\n"
           "              - (EXPERIMENTAL) maxconns_fast: immediately close new\n"
           "                connections if over maxconns limit\n"
           "              - hashpower: An integer multiplier for how large the hash\n"
           "                table should be. Can be grown at runtime if not big enough.\n"
           "                Set this based on \"STAT hash_power_level\" before a \n"
           "                restart.\n"
           );
    return;
}

static void usage_license(void) {
    printf(PACKAGE " " VERSION "\n\n");
    printf(
    "Copyright (c) 2003, Danga Interactive, Inc. <http://www.danga.com/>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions are\n"
    "met:\n"
    "\n"
    "    * Redistributions of source code must retain the above copyright\n"
    "notice, this list of conditions and the following disclaimer.\n"
    "\n"
    "    * Redistributions in binary form must reproduce the above\n"
    "copyright notice, this list of conditions and the following disclaimer\n"
    "in the documentation and/or other materials provided with the\n"
    "distribution.\n"
    "\n"
    "    * Neither the name of the Danga Interactive nor the names of its\n"
    "contributors may be used to endorse or promote products derived from\n"
    "this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS\n"
    "\"AS IS\" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT\n"
    "LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR\n"
    "A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT\n"
    "OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,\n"
    "SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT\n"
    "LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE\n"
    "OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    "\n"
    "\n"
    "This product includes software developed by Niels Provos.\n"
    "\n"
    "[ libevent ]\n"
    "\n"
    "Copyright 2000-2003 Niels Provos <provos@citi.umich.edu>\n"
    "All rights reserved.\n"
    "\n"
    "Redistribution and use in source and binary forms, with or without\n"
    "modification, are permitted provided that the following conditions\n"
    "are met:\n"
    "1. Redistributions of source code must retain the above copyright\n"
    "   notice, this list of conditions and the following disclaimer.\n"
    "2. Redistributions in binary form must reproduce the above copyright\n"
    "   notice, this list of conditions and the following disclaimer in the\n"
    "   documentation and/or other materials provided with the distribution.\n"
    "3. All advertising materials mentioning features or use of this software\n"
    "   must display the following acknowledgement:\n"
    "      This product includes software developed by Niels Provos.\n"
    "4. The name of the author may not be used to endorse or promote products\n"
    "   derived from this software without specific prior written permission.\n"
    "\n"
    "THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR\n"
    "IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES\n"
    "OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.\n"
    "IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,\n"
    "INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT\n"
    "NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,\n"
    "DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY\n"
    "THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT\n"
    "(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF\n"
    "THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.\n"
    );

    return;
}

static void save_pid(const char *pid_file) {
    FILE *fp;
    if (access(pid_file, F_OK) == 0) {
        if ((fp = fopen(pid_file, "r")) != NULL) {
            char buffer[1024];
            if (fgets(buffer, sizeof(buffer), fp) != NULL) {
                unsigned int pid;
                if (safe_strtoul(buffer, &pid) && kill((pid_t)pid, 0) == 0) {
                    fprintf(stderr, "WARNING: The pid file contained the following (running) pid: %u\n", pid);
                }
            }
            fclose(fp);
        }
    }

    if ((fp = fopen(pid_file, "w")) == NULL) {
        vperror("Could not open the pid file %s for writing", pid_file);
        return;
    }

    fprintf(fp,"%ld\n", (long)getpid());
    if (fclose(fp) == -1) {
        vperror("Could not close the pid file %s", pid_file);
    }
}

static void remove_pidfile(const char *pid_file) {
  if (pid_file == NULL)
      return;

  if (unlink(pid_file) != 0) {
      vperror("Could not remove the pid file %s", pid_file);
  }

}
static void sig_handler(const int sig)
{
    printf("SIGINT handled.\n");
    exit(EXIT_SUCCESS);
}
#ifndef HAVE_SIGIGNORE
static int sigignore(int sig) {
    struct sigaction sa = { .sa_handler = SIG_IGN, .sa_flags = 0 };

    if (sigemptyset(&sa.sa_mask) == -1 || sigaction(sig, &sa, 0) == -1) {
        return -1;
    }
    return 0;
}
#endif


/*
 * On systems that supports multiple page sizes we may reduce the
 * number of TLB-misses by using the biggest available page size
 */
static int enable_large_pages(void) {
#if defined(HAVE_GETPAGESIZES) && defined(HAVE_MEMCNTL)
    int ret = -1;
    size_t sizes[32];
    int avail = getpagesizes(sizes, 32);
    if (avail != -1) {
        size_t max = sizes[0];
        struct memcntl_mha arg = {0};
        int ii;

        for (ii = 1; ii < avail; ++ii) {
            if (max < sizes[ii]) {
                max = sizes[ii];
            }
        }

        arg.mha_flags   = 0;
        arg.mha_pagesize = max;
        arg.mha_cmd = MHA_MAPSIZE_BSSBRK;

        if (memcntl(0, 0, MC_HAT_ADVISE, (caddr_t)&arg, 0, 0) == -1) {
            fprintf(stderr, "Failed to set large pages: %s\n",
                    strerror(errno));
            fprintf(stderr, "Will use default page size\n");
        } else {
            ret = 0;
        }
    } else {
        fprintf(stderr, "Failed to get supported pagesizes: %s\n",
                strerror(errno));
        fprintf(stderr, "Will use default page size\n");
    }

    return ret;
#else
    return -1;
#endif
}

/**
 * Do basic sanity check of the runtime environment
 * @return true if no errors found, false if we can't use this env
 */
static bool sanitycheck(void) {
    /* One of our biggest problems is old and bogus libevents */
    const char *ever = event_get_version();
    if (ever != NULL) {
        if (strncmp(ever, "1.", 2) == 0) {
            /* Require at least 1.3 (that's still a couple of years old) */
            if ((ever[2] == '1' || ever[2] == '2') && !isdigit(ever[3])) {
                fprintf(stderr, "You are using libevent %s.\nPlease upgrade to"
                        " a more recent version (1.3 or newer)\n",
                        event_get_version());
                return false;
            }
        }
    }

    return true;
}

int main (int argc, char **argv)
{
    int c;
    bool lock_memory = false;
    bool do_daemonize = false;
    bool preallocate = false;
    int maxcore = 0;
    char *username = NULL;
    char *pid_file = NULL;
    struct passwd *pw;
    struct rlimit rlim;
    char unit = '\0';
    int size_max = 0;
    int retval = EXIT_SUCCESS;
    /* listening sockets */
    static int *l_socket = NULL;
    /* udp socket */
    static int *u_socket = NULL;
    bool protocol_specified = false;
    bool tcp_specified = false;
    bool udp_specified = false;
    char *subopts;
    char *subopts_value;
    enum
    {
        MAXCONNS_FAST = 0,
        HASHPOWER_INIT,
        SLAB_REASSIGN,
        SLAB_AUTOMOVE
    };
    char *const subopts_tokens[] =
    {
        [MAXCONNS_FAST] = "maxconns_fast",
        [HASHPOWER_INIT] = "hashpower",
        [SLAB_REASSIGN] = "slab_reassign",
        [SLAB_AUTOMOVE] = "slab_automove",
        NULL
    };
    if (!sanitycheck())
    {
        return EX_OSERR;
    }
    /* 遇到SIGINT(来自键盘的中断信号 ( ctrl + c ))时，终止程序 */
    signal(SIGINT, sig_handler);
    /* init settings */
    settings_init();
    setbuf(stderr, NULL);//关闭标准错误输出的缓存(for running under, say, daemontools)
    /* process arguments */
    while (-1 != (c = getopt(argc, argv,
    		"a:" //unix socket的权限位信息，unix socket的权限位信息和普通文件的权限位信息一样
    		"p:" //memcached监听的TCP端口值，默认是11211
    		"s:" //unix socket监听的socket文件路径
    		"U:" //memcached监听的UDP端口值，默认是11211
    		"m:" //memcached使用的最大内存值，默认是64M
    		"M" //当memcached的内存使用完时，不进行LRU淘汰数据，直接返回错误，该选项就是关闭LRU
    		"c:" //memcached的最大连接数,如果不指定，按系统的最大值进行
    		"k" //是否锁定memcached所持有的内存，如果锁定了内存，其他业务持有的内存就会减小
    		"hi" //帮助信息
    		"r" //core文件的大小，如果不指定，按系统的最大值进行
    		"v" //调试信息
    		"d" //设定以daemon方式运行
    		"l:" //绑定的ip信息，如果服务器有多个ip，可以在多个ip上面启动多个Memcached实例，注意：这个不是可接收的IP地址
    		"u:" //memcached运行的用户，如果以root启动，需要指定用户，否则程序错误，退出。
    		"P:" //memcached以daemon方式运行时，保存pid的文件路径信息
    		"f:" //内存的扩容因子，这个关系到Memcached内部初始化空间时的一个变化，后面详细说明
    		"n:" //chunk的最小大小(byte)，后续的增长都是该值*factor来进行增长的
    		"t:" //内部worker线程的个数，默认是4个，最大值推荐不超过64个
    		"D:" //内部数据存储时的分割符
    		"L" //指定内存页的大小，默认内存页大小为4K，页最大不超过2M，调大页的大小，可有效减小页表的大小,提高内存访问的效率
    		"R:" //单个worker的最大请求个数
    		"C" //禁用业务的cas,即compare and set
    		"b:" //listen操作缓存连接个数
    		"B:" //memcached内部使用的协议，支持二进制协议和文本协议，早期只有文本协议，二进制协议是后续加上的
    		"I:" //单个item的最大值，默认是1M,可以修改，修改的最小值为1k,最大值不能超过128M
    		"S" //打开sasl安全协议
    		"o:" /*有四个参数项可以设置:maxconns_fast(如果连接数超过最大连接数，立即关闭新的连接)hashpower
    		(hash表的大小的指数值，是按1<<hashpower来创建hash表的，默认的hashpower为16，配置值建议不超过64)
    		slab_reassign（是否调整/平衡各个slab所占的内存）slab_automove（是否自动移动各个slab，如果该选项打开，
    		会有专门的线程来进行slab的调整）*/

        ))) {
        switch (c) {
        case 'a'://修改unix socket的权限位信息
            /* access for unix domain socket, as octal mask (like chmod)*/
            settings.access= strtol(optarg,NULL,8);
            break;

        case 'U'://udp端口信息
            settings.udpport = atoi(optarg);
            udp_specified = true;
            break;
        case 'p'://tcp端口信息
            settings.port = atoi(optarg);
            tcp_specified = true;
            break;
        case 's':
            settings.socketpath = optarg;
            break;
        case 'm':
            settings.maxbytes = ((size_t)atoi(optarg)) * 1024 * 1024;
            break;
        case 'M':
            settings.evict_to_free = 0;
            break;
        case 'c':
            settings.maxconns = atoi(optarg);
            break;
        case 'h':
            usage();
            exit(EXIT_SUCCESS);
        case 'i':
            usage_license();
            exit(EXIT_SUCCESS);
        case 'k':
            lock_memory = true;
            break;
        case 'v':
            settings.verbose++;
            break;
        case 'l':
            if (settings.inter != NULL)
            {
                size_t len = strlen(settings.inter) + strlen(optarg) + 2;
                char *p = malloc(len);
                if (p == NULL) {
                    fprintf(stderr, "Failed to allocate memory\n");
                    return 1;
                }
                snprintf(p, len, "%s,%s", settings.inter, optarg);
                free(settings.inter);
                settings.inter = p;
            }
            else
            {
                settings.inter= strdup(optarg);//optarg指向当前参数的指针
            }
            break;
        case 'd':
            do_daemonize = true;
            break;
        case 'r':
            maxcore = 1;
            break;
        case 'R':
            settings.reqs_per_event = atoi(optarg);
            if (settings.reqs_per_event == 0) {
                fprintf(stderr, "Number of requests per event must be greater than 0\n");
                return 1;
            }
            break;
        case 'u':
            username = optarg;
            break;
        case 'P':
            pid_file = optarg;
            break;
        case 'f':
            settings.factor = atof(optarg);
            if (settings.factor <= 1.0) {
                fprintf(stderr, "Factor must be greater than 1\n");
                return 1;
            }
            break;
        case 'n':
            settings.chunk_size = atoi(optarg);
            if (settings.chunk_size == 0) {
                fprintf(stderr, "Chunk size must be greater than 0\n");
                return 1;
            }
            break;
        case 't':
            settings.num_threads = atoi(optarg);
            if (settings.num_threads <= 0) {
                fprintf(stderr, "Number of threads must be greater than 0\n");
                return 1;
            }
            /* There're other problems when you get above 64 threads.
             * In the future we should portably detect # of cores for the
             * default.
             */
            if (settings.num_threads > 64) {
                fprintf(stderr, "WARNING: Setting a high number of worker"
                                "threads is not recommended.\n"
                                " Set this value to the number of cores in"
                                " your machine or less.\n");
            }
            break;
        case 'D':
            if (! optarg || ! optarg[0]) {
                fprintf(stderr, "No delimiter specified\n");
                return 1;
            }
            settings.prefix_delimiter = optarg[0];
            settings.detail_enabled = 1;
            break;
        case 'L' :
            if (enable_large_pages() == 0) {
                preallocate = true;
            } else {
                fprintf(stderr, "Cannot enable large pages on this system\n"
                    "(There is no Linux support as of this version)\n");
                return 1;
            }
            break;
        case 'C' :
            settings.use_cas = false;
            break;
        case 'b' :
            settings.backlog = atoi(optarg);
            break;
        case 'B':
            protocol_specified = true;
            if (strcmp(optarg, "auto") == 0) {
                settings.binding_protocol = negotiating_prot;
            } else if (strcmp(optarg, "binary") == 0) {
                settings.binding_protocol = binary_prot;
            } else if (strcmp(optarg, "ascii") == 0) {
                settings.binding_protocol = ascii_prot;
            } else {
                fprintf(stderr, "Invalid value for binding protocol: %s\n"
                        " -- should be one of auto, binary, or ascii\n", optarg);
                exit(EX_USAGE);
            }
            break;
        case 'I':
            unit = optarg[strlen(optarg)-1];
            if (unit == 'k' || unit == 'm' ||
                unit == 'K' || unit == 'M') {
                optarg[strlen(optarg)-1] = '\0';
                size_max = atoi(optarg);
                if (unit == 'k' || unit == 'K')
                    size_max *= 1024;
                if (unit == 'm' || unit == 'M')
                    size_max *= 1024 * 1024;
                settings.item_size_max = size_max;
            } else {
                settings.item_size_max = atoi(optarg);
            }
            if (settings.item_size_max < 1024) {
                fprintf(stderr, "Item max size cannot be less than 1024 bytes.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024 * 128) {
                fprintf(stderr, "Cannot set item size limit higher than 128 mb.\n");
                return 1;
            }
            if (settings.item_size_max > 1024 * 1024) {
                fprintf(stderr, "WARNING: Setting item max size above 1MB is not"
                    " recommended!\n"
                    " Raising this limit increases the minimum memory requirements\n"
                    " and will decrease your memory efficiency.\n"
                );
            }
            break;
        case 'S': /* set Sasl authentication to true. Default is false */
#ifndef ENABLE_SASL
            fprintf(stderr, "This server is not built with SASL support.\n");
            exit(EX_USAGE);
#endif
            settings.sasl = true;
            break;
        case 'o': /* It's sub-opts time! */
            subopts = optarg;

            while (*subopts != '\0') {

            switch (getsubopt(&subopts, subopts_tokens, &subopts_value)) {
            case MAXCONNS_FAST:
                settings.maxconns_fast = true;
                break;
            case HASHPOWER_INIT:
                if (subopts_value == NULL) {
                    fprintf(stderr, "Missing numeric argument for hashpower\n");
                    return 1;
                }
                settings.hashpower_init = atoi(subopts_value);
                if (settings.hashpower_init < 12) {
                    fprintf(stderr, "Initial hashtable multiplier of %d is too low\n",
                        settings.hashpower_init);
                    return 1;
                } else if (settings.hashpower_init > 64) {
                    fprintf(stderr, "Initial hashtable multiplier of %d is too high\n"
                        "Choose a value based on \"STAT hash_power_level\" from a running instance\n",
                        settings.hashpower_init);
                    return 1;
                }
                break;
            case SLAB_REASSIGN:
                settings.slab_reassign = true;
                break;
            case SLAB_AUTOMOVE:
                if (subopts_value == NULL) {
                    settings.slab_automove = 1;
                    break;
                }
                settings.slab_automove = atoi(subopts_value);
                if (settings.slab_automove < 0 || settings.slab_automove > 2) {
                    fprintf(stderr, "slab_automove must be between 0 and 2\n");
                    return 1;
                }
                break;
            default:
                printf("Illegal suboption \"%s\"\n", subopts_value);
                return 1;
            }

            }
            break;
        default:
            fprintf(stderr, "Illegal argument \"%c\"\n", c);
            return 1;
        }
    }
    /* Use one workerthread to serve each UDP port if the user specified multiple ports */
    if (settings.inter != NULL && strchr(settings.inter, ','))//给定的IP的个数大于1
    {
        settings.num_threads_per_udp = 1;
    }
    else
    {
        settings.num_threads_per_udp = settings.num_threads;
    }
    //如果支持SASL机制
    if (settings.sasl)
    {
        if (!protocol_specified)
        {
            settings.binding_protocol = binary_prot;
        }
        else
        {
            if (settings.binding_protocol != binary_prot)
            {
                fprintf(stderr, "ERROR: You cannot allow the ASCII protocol while using SASL.\n");
                exit(EX_USAGE);
            }
        }
    }
    //指定使用TCP协议
    if (tcp_specified && !udp_specified)
    {
        settings.udpport = settings.port;
    }
    //指定使用UDP协议
    else if (udp_specified && !tcp_specified)
    {
        settings.port = settings.udpport;
    }
    /*  以下是Core文件大小和进程打开文件个数限制的调整 */

    if (maxcore != 0)
    {
        struct rlimit rlim_new;//进程的资源限制结构体
        /* First try raising to infinity; if that fails, try bringingthe soft limit to the hard. */
        if (getrlimit(RLIMIT_CORE, &rlim) == 0)
        {
        	/* 变量初始化为无限制，rlim_new.rlim_cur：当前（软）限制 ；rlim_new.rlim_max：硬限制*/
            rlim_new.rlim_cur = rlim_new.rlim_max = RLIM_INFINITY;
            //如果设置失败
            if (setrlimit(RLIMIT_CORE, &rlim_new)!= 0)
            {
                /* 变量初始化为当前值的最大值 */
                rlim_new.rlim_cur = rlim_new.rlim_max = rlim.rlim_max;
                (void)setrlimit(RLIMIT_CORE, &rlim_new);
            }
        }
        //再次确认Core文件允许的大小，如果当前的Core文件的大小为0，则不允许Core文件产生，和maxcore!=0不符，程序结束
        if ((getrlimit(RLIMIT_CORE, &rlim) != 0) || rlim.rlim_cur == 0)
        {
            fprintf(stderr, "failed to ensure corefile creation\n");
            exit(EX_OSERR);
        }
    }
    //读取进程允许打开的文件数（也就是最大连接数，因为连接需要打开一个socket句柄）信息，读取失败，程序退出
    if (getrlimit(RLIMIT_NOFILE, &rlim) != 0)
    {
        fprintf(stderr, "failed to getrlimit number of files\n");
        exit(EX_OSERR);
    }
    else
    {
    	//按memcached启动时的指定的最大连接数进行设置
        rlim.rlim_cur = settings.maxconns;
        rlim.rlim_max = settings.maxconns;
        if (setrlimit(RLIMIT_NOFILE, &rlim) != 0)
        {
            fprintf(stderr, "failed to set rlimit for open files. Try starting as root or requesting smaller maxconns value.\n");
            exit(EX_OSERR);
        }
    }
    /* 以下是启动用户的选择，降低权限 */

    if (getuid() == 0 || geteuid() == 0)//uid==0表示以root运行程序
    {
        if (username == 0 || *username == '\0')//以root运行程序，同时未指定新的用户名称
        {
            fprintf(stderr, "can't run as root without the -u switch\n");
            exit(EX_USAGE);
        }
        //判断是否存在指定的用户名称
        if ((pw = getpwnam(username)) == 0)
        {
            fprintf(stderr, "can't find the user %s to switch to\n", username);
            exit(EX_NOUSER);
        }
        //按新的用户修改memcached的执行权限位
        if (setgid(pw->pw_gid) < 0 || setuid(pw->pw_uid) < 0)
        {
            fprintf(stderr, "failed to assume identity of user %s\n", username);
            exit(EX_OSERR);
        }
    }
    /* Initialize Sasl if -S was specified */
    if (settings.sasl) {
        init_sasl();
    }
    /* if we want to ensure our ability to dump core, don't chdir to / ，daemonize中可能会切换目录*/
    if (do_daemonize)
    {
        if (sigignore(SIGHUP) == -1)
        {
            perror("Failed to ignore SIGHUP");
        }
        //以daemon的方式启动
        if (daemonize(maxcore, settings.verbose) == -1)
        {
            fprintf(stderr, "failed to daemon() in order to daemonize\n");
            exit(EXIT_FAILURE);
        }
    }
    /* 锁定内存，默认分配的内存都是虚拟内存，在程序执行过程中可以按需换出，如果内存充足的话，可以锁定内存，不让系统将该进程所持有的内存换出。 */
    if (lock_memory) {
#ifdef HAVE_MLOCKALL
    	/* mlockall将进程使用的部分或者全部的地址空间锁定在物理内存中，防止其被交换到swap空间。有些对时间敏感的应用会希望全部使用物理内存，
    	以提高数据访问和操作的效率*/
        int res = mlockall(MCL_CURRENT | MCL_FUTURE);
        if (res != 0)
        {
            fprintf(stderr, "warning: -k invalid, mlockall() failed: %s\n",strerror(errno));
        }
#else
        fprintf(stderr, "warning: -k invalid, mlockall() not supported on this platform.  proceeding without.\n");
#endif
    }
    /* initialize main thread libevent instance */
    main_base = event_init();
    /* memcached统计信息的初始化，就是对结构体stat的初始化 */
    stats_init();
    //hash表的初始化，传入的参数是启动时传入的，用于初始化hash表的大小
    assoc_init(settings.hashpower_init);
    conn_init();//连接的初始化，申请200个连接池
    //内存初始化，settings.maxbytes是Memcached初始启动参数指定的内存值大小,settings.factor是内存增长因子
    slabs_init(settings.maxbytes, settings.factor, preallocate);
    /*忽略PIPE信号，PIPE信号是当网络连接一端已经断开，这时发送数据，会进行RST的重定向，再次发送数据，会触发PIPE信号，而PIPE信号的默认
    动作是退出进程，所以需要忽略该信号。*/
    if (sigignore(SIGPIPE) == -1)
    {
        perror("failed to ignore SIGPIPE; sigaction");
        exit(EX_OSERR);
    }
    /* 工作线程的初始化，Memcached采用了典型的Master-Worker的线程模式，Master就是由main线程来充当，而Worker线程则是通过Pthread创建的。
    传入线程个数和libevent的main_base实例 */
    thread_init(settings.num_threads, main_base);
    //hash表的扩容线程
    if (start_assoc_maintenance_thread() == -1)
    {
        exit(EXIT_FAILURE);
    }
    //创建两个slab线程
    if (settings.slab_reassign && start_slab_maintenance_thread() == -1)
    {
        exit(EXIT_FAILURE);
    }
    /* initialise clock event */
    clock_handler(0, 0, 0);
    /* unix域套接字的路径 */
    if (settings.socketpath != NULL)
    {
        errno = 0;
        //socket、bind、listen UNIX域套接字
        if (server_socket_unix(settings.socketpath,settings.access))
        {
            vperror("failed to listen on UNIX socket: %s", settings.socketpath);
            exit(EX_OSERR);
        }
    }
    /* create the listening socket, bind it, and init.如果socketpath为空，则表示使用的TCP/UDP,不是使用unix socket */
    if (settings.socketpath == NULL)
    {
    	//可以从环境变量读取端口文件所在的文件路径
		const char *portnumber_filename = getenv("MEMCACHED_PORT_FILENAME");
		char temp_portnumber_filename[PATH_MAX];
		FILE *portnumber_file = NULL;
		//如果端口文件不为空，则打开
        if (portnumber_filename != NULL)
        {
            snprintf(temp_portnumber_filename,sizeof(temp_portnumber_filename),"%s.lck", portnumber_filename);
            portnumber_file = fopen(temp_portnumber_filename, "a");
            if (portnumber_file == NULL)
            {
                fprintf(stderr, "Failed to open \"%s\": %s\n", temp_portnumber_filename, strerror(errno));
            }
        }
        //settings.port表示Memcached采用的是TCP协议，创建TCP Socket，监听并且绑定
        errno = 0;
        if (settings.port && server_sockets(settings.port, tcp_transport,portnumber_file))
        {
            vperror("failed to listen on TCP port %d", settings.port);
            exit(EX_OSERR);
        }
        /* initialization order: first create the listening sockets (may need root on low ports), then drop root if
        needed,then daemonise if needed, then init libevent (in some cases descriptors created by libevent wouldn't
        survive forking). */

        /* settings.udpport表示Memcached采用的是UDP协议，创建UDP Socket，监听并且绑定  */
        errno = 0;
        if (settings.udpport && server_sockets(settings.udpport, udp_transport,portnumber_file))
        {
            vperror("failed to listen on UDP port %d", settings.udpport);
            exit(EX_OSERR);
        }
        //端口文件不为空
        if (portnumber_file)
        {
        	fclose(portnumber_file);//关闭文件
        	rename(temp_portnumber_filename, portnumber_filename);//重命名端口文件
        }
    }
    /* Give the sockets a moment to open. I know this is dumb, but the error is only an advisory. */
    usleep(1000);
    if (stats.curr_conns + stats.reserved_fds >= settings.maxconns - 1)
    {
        fprintf(stderr, "Maxconns setting is too low, use -c to increase.\n");
        exit(EXIT_FAILURE);
    }
    //保存daemon进程的进程id到文件中，这样便于控制程序，读取文件内容，即可得到进程ID。
    if (pid_file != NULL) {
        save_pid(pid_file);
    }
    /* Drop privileges no longer needed */
    drop_privileges();
    /* enter the event loop */
    if (event_base_loop(main_base, 0) != 0)
    {
        retval = EXIT_FAILURE;
    }
    stop_assoc_maintenance_thread();
    /* remove the PID file if we're a daemon */
    if (do_daemonize)
        remove_pidfile(pid_file);
    /* Clean up strdup() call for bind() address */
    if (settings.inter)
      free(settings.inter);
    if (l_socket)
      free(l_socket);
    if (u_socket)
      free(u_socket);
    return retval;
}
