package net.momirealms.sparrow.data;

import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/** 运行中的数据层, 用来注册实体类型和开启跨实体事务. 停服时调用 close. */
public interface DataService {

    @NotNull
    <K> EntityType<K> registerEntityType(@NotNull String name, @NotNull EntityIdCodec<K> idCodec);

    // body 的返回值就是 future 的结果, 可以为 null. 正常返回时提交, 调用过 Transaction.rollback() 或抛出异常时回滚
    @NotNull
    <R> CompletableFuture<R> transaction(@NotNull Function<Transaction, R> body);

    void close();
}