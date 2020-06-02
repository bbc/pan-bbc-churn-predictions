pipeline {
  agent any
  stages {
    stage('Build and Test') {
      steps {
        sh 'make clean'
      }
    }
    stage('Package') {
      steps {
        sh 'make package'
      }
    }
    stage('Deployment-test') {
      when {
        branch 'test'
      }
      environment {
        ENV = 'test'
      }
      steps {
        withAWS(region: 'eu-west-1', role: 'int-jenkins-dag-s3-S3Role') {
          s3Delete(bucket: 'test-airflow-dag-deployments', path: "${env.PIPELINE_NAME}/")
          s3Upload(bucket: 'test-airflow-dag-deployments', workingDir: 'airflow/', includePathPattern: '**/*', path: "${env.PIPELINE_NAME}/")
        }

      }
    }
    stage('Deployment-live') {
      when {
        branch 'master'
      }
      steps {
        withAWS(region: 'eu-west-1', role: 'int-jenkins-dag-s3-S3Role') {
          s3Delete(bucket: 'live-airflow-dag-deployments', path: "${env.PIPELINE_NAME}/")
          s3Upload(bucket: 'live-airflow-dag-deployments', workingDir: 'airflow/', includePathPattern: '**/*', path: "${env.PIPELINE_NAME}/")
        }

      }

    }
  }
  environment {
    PIPELINE_NAME = 'insights-news-segmentation-pipeline'
  }
  triggers {
    pollSCM('H/5 * * * *')
  }
}