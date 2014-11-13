#if defined __SUNPRO_C || defined __DECC || defined __HP_cc
# pragma ident "@(#)$Header: /cvsroot/wikipedia/willow/src/bin/willow/daemon.c,v 1.1 2005/05/02 19:15:21 kateturner Exp $"
# pragma ident "$NetBSD: daemon.c,v 1.9 2003/08/07 16:42:46 agc Exp $"
#endif

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "memcached.h"

/* 把当前进程即主进程以daemon的方式启动，daemon的实现如下，该daemon没有进行2次fork，APUE上面也有说第二次fork不是必须的。*/
int daemonize(int nochdir, int noclose)
{
    int fd;
    switch (fork())
    {
    case -1://fork失败，程序结束
        return (-1);
    case 0://子进程执行下面的流程
        break;
    default://父进程安全退出
        _exit(EXIT_SUCCESS);
    }
    //setsid调用成功之后，返回新的会话的ID，调用setsid函数的进程成为新的会话的领头进程，并与其父进程的会话组和进程组脱离
    if (setsid() == -1) return (-1);
    if (nochdir == 0)
    {
    	//进程的当前目录切换到根目录下，根目录是一直存在的，其他的目录就不保证
        if(chdir("/") != 0)
        {
            perror("chdir");
            return (-1);
        }
    }
    if (noclose == 0 && (fd = open("/dev/null", O_RDWR, 0)) != -1)
    {
        if(dup2(fd, STDIN_FILENO) < 0)
        {//将标准输入重定向到/dev/null下
            perror("dup2 stdin");
            return (-1);
        }
        if(dup2(fd, STDOUT_FILENO) < 0)
        {//将标准输出重定向到/dev/null下
            perror("dup2 stdout");
            return (-1);
        }
        if(dup2(fd, STDERR_FILENO) < 0)
        {//将标准错误重定向到/dev/null下
            perror("dup2 stderr");
            return (-1);
        }
        if (fd > STDERR_FILENO)
        {
            if(close(fd) < 0)
            {//大于2的描述符都可以关闭
                perror("close");
                return (-1);
            }
        }
    }
    return (0);
}
