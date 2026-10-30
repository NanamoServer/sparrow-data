package net.momirealms.sparrow.data;

import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/** 运行中的数据层, 用来注册实体类型和开启跨实体事务. 停服时调用 close. */
public interface DataService {

    @NotNull
    <K> EntityType<K> registerEntityType(@NotNull String name, @NotNull EntityIdCodec<K> idCodec);

    @NotNull
    CompletableFuture<Boolean> transaction(@NotNull Predicate<Transaction> body);

    void close();
}