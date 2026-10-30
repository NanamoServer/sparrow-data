package net.momirealms.sparrow.data.internal.key;

import net.momirealms.sparrow.data.key.EntryKeyCodec;
import net.momirealms.sparrow.data.key.MapKey;
import net.momirealms.sparrow.data.storage.StorageKey;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

@ApiStatus.Internal
public interface MapKeyImpl<K, E, V> extends MapKey<K, E, V> {

    @NotNull
    StorageKey storageKey();

    // 所有映射键的实现都是它, 从 MapKey 强转过来就能拿到条目键和条目值的转换
    @NotNull
    EntryKeyCodec<E> entryCodec();

    @NotNull
    ValueForm<V> valueForm();
}