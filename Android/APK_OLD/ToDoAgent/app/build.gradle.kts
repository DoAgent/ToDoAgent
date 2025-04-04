plugins {
    alias(libs.plugins.android.application)
    alias(libs.plugins.kotlin.android)
    alias(libs.plugins.kotlin.compose)
    id ("androidx.navigation.safeargs.kotlin")
}

android {
    namespace = "com.asap.todoexmple"
    compileSdk = 35

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
        viewBinding = true
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
//    // HikariCP 依赖
//    implementation ("com.zaxxer:HikariCP-android:2.4.0")
//    // SLF4J 依赖 (HikariCP需要)
//    implementation ("org.slf4j:slf4j-android:1.7.36")
    implementation ("androidx.cardview:cardview:1.0.0")
    implementation ("androidx.viewpager2:viewpager2:1.0.0")


    implementation ("androidx.work:work-runtime-ktx:2.8.1")
    //外观
    implementation ("androidx.appcompat:appcompat:1.6.1")
    implementation ("com.google.android.material:material:1.9.0")
    implementation ("de.hdodenhof:circleimageview:3.1.0")
    //fragment
    implementation ("androidx.navigation:navigation-fragment-ktx:2.7.7")
    implementation ("androidx.navigation:navigation-ui-ktx:2.7.7")
    implementation ("androidx.fragment:fragment-ktx:1.6.2")

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
    implementation ("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.1")
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
    implementation(libs.androidx.recyclerview)
    implementation(libs.androidx.work.runtime.ktx)
    implementation(libs.firebase.components)
    implementation(libs.play.services.cast.framework)
    testImplementation(libs.junit)
    androidTestImplementation(libs.androidx.junit)
    androidTestImplementation(libs.androidx.espresso.core)
    androidTestImplementation(platform(libs.androidx.compose.bom))
    androidTestImplementation(libs.androidx.ui.test.junit4)
    debugImplementation(libs.androidx.ui.tooling)
    debugImplementation(libs.androidx.ui.test.manifest)

}