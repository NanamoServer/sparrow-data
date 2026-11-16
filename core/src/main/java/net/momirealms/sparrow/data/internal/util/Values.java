package net.momirealms.sparrow.data.internal.util;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

@ApiStatus.Internal
public final class Values {

    private Values() {
    }

    // 判断观察方看到的值有没有变. 按 equals 比较, 顶层的 byte[] 没有按内容比较的 equals, 单独处理
    public static boolean same(@Nullable Object previous, @Nullable Object current) {
        if (previous == current) return true;
        if (previous instanceof byte[] before && current instanceof byte[] after) return Arrays.equals(before, after);
        return previous != null && previous.equals(current);
    }
}