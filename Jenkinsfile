pipeline {
    agent any

    triggers {
        cron("H H(20-23) * * 1-5")
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: "5"))
        timeout time: 60, unit: "MINUTES"
    }

    stages {
        stage("Build") {
            steps {
                configFileProvider([configFile(fileId: "83303a32-933f-4cc9-8f6d-5cf2e85ac68d", variable: 'ENV_CONFIG')]) {
                    load "${ENV_CONFIG}"
                    withEnv(["JAVA_HOME=${ tool type: 'jdk', name: "$JDK_VERSION" }",
                     "PATH+MAVEN=${tool type: 'maven', name: "$MAVEN_VERSION"}/bin:${env.JAVA_HOME}/bin"]) {
                        sh "mvn clean compile"
                    }
                }
            }
        }

        stage("Test") {
            steps {
                configFileProvider([configFile(fileId: "83303a32-933f-4cc9-8f6d-5cf2e85ac68d", variable: 'ENV_CONFIG')]) {
                    load "${ENV_CONFIG}"
                    withEnv(["JAVA_HOME=${ tool type: 'jdk', name: "$JDK_VERSION" }",
                     "PATH+MAVEN=${tool type: 'maven', name: "$MAVEN_VERSION"}/bin:${env.JAVA_HOME}/bin"]) {
                        sh "mvn clean compile"
                    }
                }
            }
        }
    }

    post {
        always {
            warnings canComputeNew: false, canResolveRelativePaths: false, categoriesPattern: '', consoleParsers: [[parserName: 'Maven'], [parserName: 'Java Compiler (javac)']], defaultEncoding: '', excludePattern: '', healthy: '', includePattern: '', messagesPattern: '', unHealthy: ''
            junit "**/target/surefire-reports/*.xml"
            jacoco()
        }

        failure {
            script {
                def emails = sh(returnStdout: true, script: "git --no-pager log ${GIT_PREVIOUS_SUCCESSFUL_COMMIT}..${GIT_COMMIT} -s --format=%ae|sort -u").trim().split("\\n")
                emails.each { email ->
                    step([$class: 'Mailer', notifyEveryUnstableBuild: true, recipients: "$email", sendToIndividuals: true])
                }
            }
        }
    }
}
