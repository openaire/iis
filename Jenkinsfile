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
        stage("Package") {
            steps {
                configFileProvider([configFile(fileId: "iis-build-properties", variable: 'BUILD_PROPERTIES')]) {
                    load "${BUILD_PROPERTIES}"
                    withEnv(["JAVA_HOME=${ tool type: 'jdk', name: "$JDK_VERSION" }",
                             "PATH+MAVEN=${tool type: 'maven', name: "$MAVEN_VERSION"}/bin:${env.JAVA_HOME}/bin"]) {
                        withSonarQubeEnv('sonar.ceon.pl') {
                            //NOTE: sonar scan is only done for master branch because current Sonar instance does not support branching
                            sh '''
                                if [ $GIT_BRANCH = "master" ]; then
                                    mvn clean package \
                                        -DskipITs \
                                        -Djava.net.preferIPv4Stack=true \
                                        $SONAR_MAVEN_GOAL \
                                        -Dsonar.host.url=$SONAR_HOST_URL
                                else
                                    mvn clean package \
                                        -DskipITs \
                                        -Djava.net.preferIPv4Stack=true
                                fi
                            '''
                        }
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
            cleanWs()
        }

        failure {
            emailext (
                subject: "Build failed in Jenkins: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                body: "Failed job '${env.JOB_NAME}' #${env.BUILD_NUMBER}: check console output at ${env.BUILD_URL}.",
                recipientProviders:  [developers()]
            )
        }
    }
}
