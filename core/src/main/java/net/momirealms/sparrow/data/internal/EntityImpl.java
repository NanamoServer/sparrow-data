package net.momirealms.sparrow.data.internal;

import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.entity.Pin;
import net.momirealms.sparrow.data.entity.Watch;
import net.momirealms.sparrow.data.internal.key.AbstractDataKey;
import net.momirealms.sparrow.data.internal.key.DbKeyImpl;
import net.momirealms.sparrow.data.internal.key.RosterKeyImpl;
import net.momirealms.sparrow.data.internal.key.ValueKeyImpl;
import net.momirealms.sparrow.data.internal.key.ViewKeyImpl;
import net.momirealms.sparrow.data.internal.memory.HeldEntity;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.key.DbCounterKey;
import net.momirealms.sparrow.data.key.DbKey;
import net.momirealms.sparrow.data.key.MemoryDataKey;
import net.momirealms.sparrow.data.key.MemoryKey;
import net.momirealms.sparrow.data.key.RosterKey;
import net.momirealms.sparrow.data.key.ValueKey;
import net.momirealms.sparrow.data.key.ViewKey;
import net.momirealms.sparrow.data.storage.EntityRef;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

@ApiStatus.Internal
public record EntityImpl<K>(@NotNull EntityTypeImpl<K> type, @NotNull K id) implements Entity<K> {

    // 实体 ID 编码后超过 64 字节时抛 IllegalArgumentException. 每个操作都在调用线程上先调用它, 异常直接抛给调用方
    @NotNull
    public EntityRef ref() {
        return this.type.ref(this.id);
    }

    @Override
    public boolean isHeld() {
        return this.type.held(this.id) != null;
    }

    @NotNull
    @Override
    public <T> T get(@NotNull MemoryKey<K, T> key) {
        return this.requireHeld().get((ValueKeyImpl<K, T>) key);
    }

    @Override
    public <T> void write(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        this.requireHeld().write(impl, value);
        this.type.watches().touch(this.id, impl.storageKey().id());
    }

    @NotNull
    @Override
    public <T> T update(@NotNull MemoryKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        T value = this.requireHeld().update(impl, operator);
        this.type.watches().touch(this.id, impl.storageKey().id());
        return value;
    }

    // 把这个键的当前值发给别的服, 不落库, 调用方不等结果. 要多久发一次由调用方决定
    @Override
    public void publish(@NotNull MemoryDataKey<K> key) {
        this.requireHeld().publish((AbstractDataKey<K>) key);
    }

    // 排进这个实体的落库链, future 在这次存储调用完成后结束. 写完会广播写到的键的失效, 别的服重读库
    @NotNull
    @Override
    public CompletableFuture<Void> save() {
        return this.requireHeld().save();
    }

    // 任何服, 不管这个实体被谁持有都能调用. future 在写库完成后结束, 广播在后台继续
    @NotNull
    @Override
    public <T> CompletableFuture<Void> override(@NotNull MemoryKey<K, T> key, @NotNull T value) {
        ValueKeyImpl<K, T> impl = (ValueKeyImpl<K, T>) key;
        return this.type.overrides().override(this.ref(), impl.storageKey(), impl.form(), value);
    }

    // 重置是删掉存储的值, 持有者把它读成默认值或 Optional.empty()
    @NotNull
    @Override
    public CompletableFuture<Void> reset(@NotNull MemoryKey<K, ?> key) {
        ValueKeyImpl<K, ?> impl = (ValueKeyImpl<K, ?>) key;
        return this.type.overrides().override(this.ref(), impl.storageKey(), impl.form(), null);
    }

    @NotNull
    @Override
    public <T> Watch watch(@NotNull ValueKey<K, T> key, @NotNull Runnable onChange) {
        return this.type.watches().watch(this.id, key, onChange);
    }

    @NotNull
    @Override
    public <T> Pin pin(@NotNull ValueKey<K, T> key) {
        return this.type.watches().watch(this.id, key, null);
    }

    // 本服的重读在调用线程上发起, 其他服靠广播的失效重读. 本服不会收到这条广播, 所以只读一次
    @NotNull
    @Override
    public CompletableFuture<Void> invalidate(@NotNull ViewKey<K, ?> key) {
        String keyId = ((ViewKeyImpl<K, ?>) key).storageKey().id();
        EntityRef ref = this.ref();
        CompletableFuture<Void> reloaded = this.type.snapshots().reload(this.id, keyId);
        this.type.service().messenger().invalidateOthers(this.type.name(), ref.entityId(), keyId);
        return reloaded;
    }

    @NotNull
    @Override
    public <V> CompletableFuture<Void> announce(@NotNull RosterKey<K, V> key, @NotNull V value) {
        return ((RosterKeyImpl<K, V>) key).roster().announce(this.id, value);
    }

    @NotNull
    @Override
    public CompletableFuture<Void> withdraw(@NotNull RosterKey<K, ?> key) {
        return ((RosterKeyImpl<K, ?>) key).roster().withdraw(this.id);
    }

    @Nullable
    @Override
    public <V> V member(@NotNull RosterKey<K, V> key) {
        return ((RosterKeyImpl<K, V>) key).roster().member(this.id);
    }

    @NotNull
    @Override
    public Watch watch(@NotNull RosterKey<K, ?> key, @NotNull Runnable onChange) {
        return ((RosterKeyImpl<K, ?>) key).roster().watch(this.id, onChange);
    }

    // 内存键在本服持有时直接读容器, 其他情况读快照
    @Nullable
    @Override
    public <T> T peek(@NotNull ValueKey<K, T> key) {
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return held.get((ValueKeyImpl<K, T>) key);
        return this.type.snapshots().peek(this.id, ((AbstractDataKey<K>) key).storageKey().id(), this.type.reader(this.id, key));
    }

    @NotNull
    @Override
    public <T> CompletableFuture<T> read(@NotNull ValueKey<K, T> key) {
        HeldEntity<K> held = this.heldFor(key);
        if (held != null) return CompletableFuture.completedFuture(held.get((ValueKeyImpl<K, T>) key));
        return this.type.reader(this.id, key).get();
    }

    @NotNull
    @Override
    public <T> CompletableFuture<Void> set(@NotNull DbKey<K, T> key, @NotNull T value) {
        return this.type.db().setValue(this.id, (DbKeyImpl<K, T>) key, value);
    }

    @NotNull
    @Override
    public <T> CompletableFuture<T> update(@NotNull DbKey<K, T> key, @NotNull UnaryOperator<T> operator) {
        return this.type.db().updateValue(this.id, (DbKeyImpl<K, T>) key, operator);
    }

    @NotNull
    @Override
    public CompletableFuture<Void> set(@NotNull DbCounterKey<K> key, long value) {
        return this.type.db().setCount(this.id, ((ValueKeyImpl<K, Long>) key).storageKey(), value);
    }

    @NotNull
    @Override
    public CompletableFuture<Long> add(@NotNull DbCounterKey<K> key, long delta) {
        return this.type.db().add(this.id, ((ValueKeyImpl<K, Long>) key).storageKey(), delta);
    }

    @NotNull
    @Override
    public CompletableFuture<OptionalLong> take(@NotNull DbCounterKey<K> key, long amount) {
        return this.type.db().take(this.id, ((ValueKeyImpl<K, Long>) key).storageKey(), amount);
    }

    // 删除这个实体的全部数据. 在 Executor 上删库, 删完广播这个实体全部键的失效, future 在删库完成后结束.
    // 本服持有时抛 IllegalStateException, 持有方下次落库会把数据写回去; 别的服不持有由调用方保证
    @NotNull
    @Override
    public CompletableFuture<Void> delete() {
        if (this.type.held(this.id) != null) throw new IllegalStateException("Entity is held by this server: " + this.type.name() + " " + this.id);
        EntityRef ref = this.ref();
        DataServiceImpl service = this.type.service();
        return CompletableFuture.runAsync(
                () -> {
                    this.type.prepareStorage();
                    service.storage().deleteEntity(ref);
                },
                service.executor()
        )
                .thenRun(() -> service.messenger().invalidate(this.type.name(), ref.entityId(), null));
    }

    // 内存键的读写只有本服持有这个实体时才能用
    @NotNull
    private HeldEntity<K> requireHeld() {
        HeldEntity<K> held = this.type.held(this.id);
        if (held == null) throw new IllegalStateException("Entity is not held by this server: " + this.type.name() + " " + this.id);
        return held;
    }

    // 内存键在本服持有时返回它的容器, 其他情况返回 null
    @Nullable
    private HeldEntity<K> heldFor(@NotNull DataKey<K> key) {
        return key instanceof MemoryDataKey<?> ? this.type.held(this.id) : null;
    }
}