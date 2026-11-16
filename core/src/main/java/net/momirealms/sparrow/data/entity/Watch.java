package net.momirealms.sparrow.data.entity;

/**
 * 带变化通知的 {@link Pin}.
 *
 * <p>被观察的值和上次通知时不 equals 才调用 onChange, 顶层的 byte[] 按内容比较. 回调在触发改动的线程上同步执行,
 * 可能是调用 write 的线程, Redis 的订阅线程或宿主的 Executor, 要快, 并且线程安全.
 * 关闭之后不再通知, 注销被观察的键时数据层会通知最后一次再自动关闭.
 */
public interface Watch extends Pin {
}