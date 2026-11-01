package net.momirealms.sparrow.data.entity;

public interface Watch extends AutoCloseable {

    // 关闭之后不再通知, 重复关闭没有效果. 注销被观察的键时数据层会通知最后一次再自动关闭
    @Override
    void close();
}