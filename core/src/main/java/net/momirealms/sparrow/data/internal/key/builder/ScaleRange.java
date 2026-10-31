package net.momirealms.sparrow.data.internal.key.builder;

final class ScaleRange {

    private ScaleRange() {
    }

    // 10 的 18 次方是 long 能放下的最大的 10 的幂
    static int require(int scale) {
        if (scale < 0 || scale > 18) throw new IllegalArgumentException("Scale must be between 0 and 18: " + scale);
        return scale;
    }
}