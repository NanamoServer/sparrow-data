package net.momirealms.sparrow.data.entity;

public interface Watch extends AutoCloseable {

    // 被观察的值可能变了时调用 onChange. 回调在触发改动的线程上同步执行, 可能是调用 write 的线程, Redis 的订阅线程或宿主的 Executor,
    // 要快, 并且线程安全. 关闭之后不再通知, 重复关闭没有效果. 注销被观察的键时数据层会通知最后一次再自动关闭
    @Override
    void close();
}