def dockerImage = 'docker-registry.ocean.icm.edu.pl/eu.openaire.iis.builder:latest'
def dockerArgs(env) { return "-e JAVA_TOOL_OPTIONS=-Duser.home=${env.WORKSPACE}"; }

pipeline {
    agent any

    triggers {
        cron('H H(20-23) * * 1-5')
	pollSCM('H * * * 1-5')
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '5'))
	lock('iis-build')
	timeout time: 60, unit: 'MINUTES'
    }

    environment {
	ARTIFACTORY_DEPLOY=credentials('artifactory-deploy')
    }
    stages {
        stage('Build') {
            agent {
                docker {
                    image dockerImage
		    reuseNode true
		    args dockerArgs(env)
                }
            }
            steps {
		sh '''
                    mvn -B clean install -DskipITs -DotherExcludedGroups=SlowTest \
                        -Djunit.jupiter.testmethod.order.default=org.junit.jupiter.api.MethodOrderer\\$Random \
                        -Djunit.jupiter.testclass.order.default=org.junit.jupiter.api.ClassOrderer\\$Random \
                        -Dmaven.test.failure.ignore
                    '''
            }
	    post {
		always {
		    recordIssues(tools: [mavenConsole(), java()])
		    junit "**/target/surefire-reports/*.xml"
		    jacoco()
		}
	    }
        }
        stage('Deploy') {
            agent {
                docker {
                    image dockerImage
		    reuseNode true
		    args dockerArgs(env)
                }
            }
	    when { branch 'master' }
            steps {
		sh 'mvn -B deploy -s deploy-settings.xml -DskipTests'
            }
        }
    }

    post {
        unstable {
            emailext (
                subject: "Build unstable in Jenkins: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                body: "Unstable job '${env.JOB_NAME}' #${env.BUILD_NUMBER}: check console output at ${env.BUILD_URL}.",
                recipientProviders:  [developers()]
            )
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
