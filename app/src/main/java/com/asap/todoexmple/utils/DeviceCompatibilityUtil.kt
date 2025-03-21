import android.content.Context
import android.os.Build

object DeviceCompatibilityUtil {
    private const val OPLUS_MANUFACTURER = "OPLUS"
    private const val REALME_MANUFACTURER = "realme"
    private const val ONEPLUS_MANUFACTURER = "OnePlus"

    fun isColorOSDevice(): Boolean {
        return Build.MANUFACTURER.uppercase() in listOf(
            OPLUS_MANUFACTURER,
            REALME_MANUFACTURER,
            ONEPLUS_MANUFACTURER
        )
    }

    fun initializeColorOSFeatures(context: Context) {
        if (isColorOSDevice()) {
            // 初始化 ColorOS 特定功能
        }
    }
} 