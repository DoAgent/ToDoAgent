plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
}

android {
    namespace = "com.asap.todoexmple"
    compileSdk = 34

    defaultConfig {
        applicationId = "com.asap.todoexmple"
        minSdk = 29
        targetSdk = 35
        versionCode = 1
        versionName = "1.0"

        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(
                getDefaultProguardFile("proguard-android-optimize.txt"),
                "proguard-rules.pro"
            )
        }
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }
    kotlinOptions {
        jvmTarget = "11"
    }
    buildFeatures {
        compose = true
    }
//    packagingOptions {
//        exclude ("META-INF/DEPENDENCIES")
//        exclude ("META-INF/LICENSE")
//        exclude ("META-INF/LICENSE.txt")
//        exclude ("META-INF/license.txt")
//        exclude ("META-INF/NOTICE")
//        exclude ("META-INF/NOTICE.txt")
//        exclude ("META-INF/notice.txt")
//        exclude ("META-INF/ASL2.0")
//        exclude ("META-INF/**")
//    }
}

dependencies {
    implementation("androidx.lifecycle:lifecycle-viewmodel-ktx:2.7.0")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.7.0")
    //implementation("androidx.localbroadcastmanager:localbroadcastmanager:1.1.0")
    ////哟下数据库依赖
    implementation("com.squareup.retrofit2:retrofit:2.9.0")
    implementation("com.squareup.retrofit2:converter-gson:2.9.0")
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:logging-interceptor:4.12.0")
    ////
    implementation ("mysql:mysql-connector-java:5.1.49")  // 使用较老但稳定的版本
//    implementation("org.mariadb.jdbc:mariadb-java-client:3.0.3")
//    // 如果遇到 minify 问题，添加以下配置
//    implementation ("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.1")
    ////
    implementation(libs.androidx.core.ktx)
    implementation(libs.androidx.lifecycle.runtime.ktx)
    implementation(libs.androidx.activity.compose)
    implementation(platform(libs.androidx.compose.bom))
    implementation(libs.androidx.ui)
    implementation(libs.androidx.ui.graphics)
    implementation(libs.androidx.ui.tooling.preview)
    implementation(libs.androidx.material3)
    implementation(libs.androidx.constraintlayout)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.ui.test.junit4)
    debugImplementation(libs.androidx.ui.tooling)
    debugImplementation(libs.androidx.ui.test.manifest)

}