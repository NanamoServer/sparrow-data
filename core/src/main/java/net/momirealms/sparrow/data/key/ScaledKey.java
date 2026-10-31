package net.momirealms.sparrow.data.key;

import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.RoundingMode;

public interface ScaledKey {

    // 带精度的键. 键收发的 long 是最小单位, 数值等于最小单位除以 10 的 scale 次方.
    // 精度由构建器的 scale(int) 设置, 取 0 到 18, 不设时为 0. 注册后不能改, 各服要用同一个精度
    int scale();

    // 小数位比精度多时按 mode 舍入, 传 UNNECESSARY 时抛 ArithmeticException. 超出 long 范围也抛 ArithmeticException
    default long toUnits(@NotNull BigDecimal amount, @NotNull RoundingMode mode) {
        return amount.setScale(this.scale(), mode).unscaledValue().longValueExact();
    }

    default long toUnits(double amount, @NotNull RoundingMode mode) {
        long unit = unit(this.scale());
        double scaled = amount * unit;
        // 最小单位在 2^51 以内时, 换回 double 和 amount 相等说明 amount 本来就不超过 scale 位小数, 不用舍入
        if (Math.abs(scaled) < 0x1p51) {
            long units = Math.round(scaled);
            if ((double) units / unit == amount) return units;
        }
        // 其他情况按 amount 打印出来的十进制数舍入, 例如 1.005 按 HALF_UP 保留两位得到 1.01
        return this.toUnits(BigDecimal.valueOf(amount), mode);
    }

    // 小数位数总是 scale, 余额是 1.80 而不是 1.8
    @NotNull
    default BigDecimal toDecimal(long units) {
        return BigDecimal.valueOf(units, this.scale());
    }

    default double toDouble(long units) {
        return (double) units / unit(this.scale());
    }

    private static long unit(int scale) {
        long unit = 1;
        for (int i = 0; i < scale; i++) {
            unit *= 10;
        }
        return unit;
    }
}