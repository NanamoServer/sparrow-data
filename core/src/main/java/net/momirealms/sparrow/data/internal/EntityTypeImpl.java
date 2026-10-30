package net.momirealms.sparrow.data.internal;

import com.mojang.serialization.Codec;
import net.momirealms.sparrow.data.Key;
import net.momirealms.sparrow.data.RankEntry;
import net.momirealms.sparrow.data.entity.Entity;
import net.momirealms.sparrow.data.entity.EntityIdCodec;
import net.momirealms.sparrow.data.entity.EntityType;
import net.momirealms.sparrow.data.entity.Residence;
import net.momirealms.sparrow.data.internal.key.KeyRegistry;
import net.momirealms.sparrow.data.internal.key.ValueForm;
import net.momirealms.sparrow.data.internal.key.builder.DbCounterKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbMapCounterKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.DbMapKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryLongKeyBuilderImpl;
import net.momirealms.sparrow.data.internal.key.builder.MemoryMapKeyBuilderImpl;
import net.momirealms.sparrow.data.key.DataKey;
import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.SortableKey;
import net.momirealms.sparrow.data.key.builder.DbCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbMapCounterKeyBuilder;
import net.momirealms.sparrow.data.key.builder.DbMapKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryLongKeyBuilder;
import net.momirealms.sparrow.data.key.builder.MemoryMapKeyBuilder;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@ApiStatus.Internal
public final class EntityTypeImpl<K> implements EntityType<K> {
    private final String name;
    private final EntityIdCodec<K> idCodec;
    private final KeyRegistry<K> keys = new KeyRegistry<>();

    // 一个实体类型在运行时的实现, 注册的键都登记在它自己的键表里
    public EntityTypeImpl(@NotNull String name, @NotNull EntityIdCodec<K> idCodec) {
        this.name = name;
        this.idCodec = idCodec;
    }

    @NotNull
    @Override
    public String name() {
        return this.name;
    }

    @NotNull
    @Override
    public Entity<K> entity(@NotNull K id) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public Residence<K> residence() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public <T> MemoryKeyBuilder<K, T> memory(@NotNull Key id, @NotNull Codec<T> codec, @NotNull T defaultValue) {
        return new MemoryKeyBuilderImpl<>(this, id, new ValueForm.NbtForm<>(codec, defaultValue));
    }

    @NotNull
    @Override
    public <T> MemoryKeyBuilder<K, Optional<T>> memory(@NotNull Key id, @NotNull Codec<T> codec) {
        return new MemoryKeyBuilderImpl<>(this, id, new ValueForm.OptionalNbtForm<>(codec));
    }

    @NotNull
    @Override
    public MemoryLongKeyBuilder<K> memoryLong(@NotNull Key id) {
        return new MemoryLongKeyBuilderImpl<>(this, id);
    }

    // 映射键的条目没有默认值, 条目不存在时读到 null
    @NotNull
    @Override
    public <E, V> MemoryMapKeyBuilder<K, E, V> memoryMap(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull Codec<V> valueCodec) {
        return new MemoryMapKeyBuilderImpl<>(this, id, entryCodec, new ValueForm.NbtForm<>(valueCodec, null));
    }

    @NotNull
    @Override
    public <T> DbKeyBuilder<K, T> db(@NotNull Key id, @NotNull Codec<T> codec, @NotNull T defaultValue) {
        return new DbKeyBuilderImpl<>(this, id, new ValueForm.NbtForm<>(codec, defaultValue));
    }

    @NotNull
    @Override
    public <T> DbKeyBuilder<K, Optional<T>> db(@NotNull Key id, @NotNull Codec<T> codec) {
        return new DbKeyBuilderImpl<>(this, id, new ValueForm.OptionalNbtForm<>(codec));
    }

    @NotNull
    @Override
    public DbCounterKeyBuilder<K> dbCounter(@NotNull Key id) {
        return new DbCounterKeyBuilderImpl<>(this, id);
    }

    @NotNull
    @Override
    public <E, V> DbMapKeyBuilder<K, E, V> dbMap(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec, @NotNull Codec<V> valueCodec) {
        return new DbMapKeyBuilderImpl<>(this, id, entryCodec, new ValueForm.NbtForm<>(valueCodec, null));
    }

    @NotNull
    @Override
    public <E> DbMapCounterKeyBuilder<K, E> dbMapCounter(@NotNull Key id, @NotNull EntryKeyCodec<E> entryCodec) {
        return new DbMapCounterKeyBuilderImpl<>(this, id, entryCodec);
    }

    @NotNull
    @Override
    public CompletableFuture<Void> unregister(@NotNull DataKey<K> key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<Void> purge(@NotNull Key id) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<List<RankEntry<K>>> top(@NotNull SortableKey<K> key, int offset, int limit) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    @Override
    public CompletableFuture<Long> rankOf(@NotNull SortableKey<K> key, @NotNull K id) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @NotNull
    public EntityIdCodec<K> idCodec() {
        return this.idCodec;
    }

    @NotNull
    public KeyRegistry<K> keys() {
        return this.keys;
    }
}