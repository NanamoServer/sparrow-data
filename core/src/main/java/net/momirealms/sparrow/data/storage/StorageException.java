package net.momirealms.sparrow.data.storage;

import org.jetbrains.annotations.NotNull;

public class StorageException extends RuntimeException {

    // 存储实现遇到底层故障时抛出, 原始异常放在 cause 里. 宿主自己写的存储实现也用它
    public StorageException(@NotNull String message, @NotNull Throwable cause) {
        super(message, cause);
    }
}