package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.key.ValueKey;
import net.momirealms.sparrow.data.storage.StorageKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public interface ValueKeyImpl<K, T> extends ValueKey<K, T> {

    @NotNull
    StorageKey storageKey();

    // 所有普通键的实现都是它, 从 ValueKey 强转过来就能拿到值和存储之间的转换, 计数键的 T 是 Long
    @NotNull
    ValueForm<T> form();
}