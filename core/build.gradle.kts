plugins {
    id("maven-publish")
}

dependencies {
    compileOnly(libs.sparrow.message)
    compileOnly(libs.netty.buffer)
    compileOnly(libs.caffeine)
    compileOnly(libs.slf4j)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            artifactId = "sparrow-data"
            from(components["java"])
            version = project.version.toString()
            pom {
                name = "Sparrow Data"
                url = "https://github.com/Catnies/sparrow-data"
            }
        }
    }

    repositories {
        maven {
            name = "XiaoMoMi"
            url = uri("https://repo.momirealms.net/snapshots")
            credentials(PasswordCredentials::class)
            authentication {
                create<BasicAuthentication>("basic")
            }
        }
    }
}
