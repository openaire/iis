def MAVEN_VERSION = "Maven3"
def JDK_VERSION = "Sun JDK 8"

def TRIGGERS_CRON = "H H(20-23) * * 1-5"

def BUILD_NUMBER_TO_KEEP = "5"
def BUILD_TIMEOUT_VALUE = 60
def BUILD_TIMEOUT_UNIT = "MINUTES"

pipeline {
    agent any

    tools {
        maven MAVEN_VERSION
        jdk JDK_VERSION
    }

    triggers {
        cron(TRIGGERS_CRON)
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: BUILD_NUMBER_TO_KEEP))
        timeout time: BUILD_TIMEOUT_VALUE, unit: BUILD_TIMEOUT_UNIT
    }

    stages {
        stage("Build") {
            steps {
                sh "mvn clean compile"
            }
        }
        stage("Test") {
            steps {
                sh "mvn test"
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
