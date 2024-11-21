import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import {MSKContruct} from "./msk-construct";
import * as cdk from 'aws-cdk-lib';
import {Asset} from "aws-cdk-lib/aws-s3-assets";
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import {aws_logs, CustomResource, RemovalPolicy} from 'aws-cdk-lib';
import * as kda from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import * as aps from 'aws-cdk-lib/aws-aps';
import * as python from '@aws-cdk/aws-lambda-python-alpha';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import {Provider} from "aws-cdk-lib/custom-resources";
import {SubnetType} from "aws-cdk-lib/aws-ec2";

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);


    const vehicleEventFlinkApplicationAsset = new Asset(this, 'VehicleEventFlinkApplicationAsset', {
      path: '../',
      bundling: {
        image: cdk.DockerImage.fromRegistry('maven:3.8.6-openjdk-11'),
        command: [
          'sh',
          '-c',
          'cd vehicle-event-generator && mvn clean package && cp target/[^original-]*.jar /asset-output/'],
        outputType: cdk.BundlingOutput.SINGLE_FILE, // Bundling output will be zipped even though it produces a single archive file.
      },
    });

    const preProcessorFlinkApplicationAsset = new Asset(this, 'PreProcessorFlinkApplicationAsset', {
      path: '../',
      bundling: {
        image: cdk.DockerImage.fromRegistry('maven:3.8.6-openjdk-11'),
        command: [
          'sh',
          '-c',
          'cd pre-processor && mvn clean package && cp target/[^original-]*.jar /asset-output/'],
        outputType: cdk.BundlingOutput.SINGLE_FILE, // Bundling output will be zipped even though it produces a single archive file.
      },
    });

    const kafkaClientLayer = new python.PythonLayerVersion(this, 'kafkaLayer', {
      entry: './lambda-python-layer/',
      compatibleArchitectures: [lambda.Architecture.X86_64],
      compatibleRuntimes: [lambda.Runtime.PYTHON_3_11],
      bundling: {
        platform: 'linux/amd64',
      }
    })

    const vpc = new ec2.Vpc(this, 'VPC', {
      enableDnsHostnames: true,
      enableDnsSupport: true,
      maxAzs: 3,
      natGateways: 1,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: 'public-subnet',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          cidrMask: 24,
          name: 'private-subnet',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        }
      ],
    });

    // security group for MSK access
    const mskSG = new ec2.SecurityGroup(this, 'mskSG', {
      vpc: vpc,
      allowAllOutbound: true,
      description: 'MSK Security Group'
    });

    mskSG.connections.allowInternally(ec2.Port.allTraffic(), 'Allow all traffic between hosts having the same security group');


    const prometheusEndpoint = new ec2.InterfaceVpcEndpoint(this, 'Prometheus endpoint', {
          vpc,
          service: new ec2.InterfaceVpcEndpointService('com.amazonaws.'+this.region+'.aps-workspaces'),
          securityGroups: [mskSG],
          privateDnsEnabled: true,
        }
    );

    const prometheusWorkspace = new aps.CfnWorkspace(this, 'prometheusWorkspace', /* all optional props */ {
      alias: 'prometheus',
    });


    //AccessVPCPolicy
    const accessVPCPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['*'],
          actions: ['ec2:DeleteNetworkInterface',
            'ec2:DescribeDhcpOptions',
            'ec2:DescribeSecurityGroups',
            'ec2:CreateNetworkInterface',
            'ec2:DescribeNetworkInterfaces',
            'ec2:CreateNetworkInterfacePermission',
            'ec2:DescribeVpcs',
            'ec2:DescribeSubnets'],
        }),
      ],
    });

    // our KDA app needs access to describe kinesisanalytics
    const kdaAccessPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:kinesisanalytics:"+this.region+":"+this.account+":application/kds-msf-aoss"],
          actions: ['kinesisanalyticsv2:DescribeApplication','kinesisAnalyticsv2:UpdateApplication']
        }),
      ],
    });

    const vehicleLogGroup = new logs.LogGroup(this, 'Vehicle LogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      logGroupName: this.stackName+"-vehicle-log-group",// Adjust retention as needed
      removalPolicy: RemovalPolicy.DESTROY
    });

    const vehicleLogStream = new logs.LogStream(this, 'Vehicle LogStream', {
      logGroup: vehicleLogGroup,
    });

    const accessCWLogsPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+vehicleLogGroup.logGroupName,
            "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+vehicleLogGroup.logGroupName+":log-stream:" + vehicleLogStream.logStreamName],
          actions: ['logs:PutLogEvents','logs:DescribeLogGroups','logs:DescribeLogStreams','cloudwatch:PutMetricData'],
        }),
      ],
    });


    const preProcessorlogGroup = new logs.LogGroup(this, 'PreProcessorMyLogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      logGroupName: this.stackName+"-pre-processor-log-group",// Adjust retention as needed
      removalPolicy: RemovalPolicy.DESTROY
    });

    const preProcessorlogStream = new logs.LogStream(this, 'preProcessorLogStream', {
      logGroup: preProcessorlogGroup,
    });

    const preProcessorAccessCWLogsPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+preProcessorlogGroup.logGroupName,
            "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+preProcessorlogGroup.logGroupName+":log-stream:" + preProcessorlogStream.logStreamName],
          actions: ['logs:PutLogEvents','logs:DescribeLogGroups','logs:DescribeLogStreams','cloudwatch:PutMetricData'],
        }),
      ],
    });

    const remoteWritePolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: [prometheusWorkspace.attrArn],
          actions: ['aps:RemoteWrite']
        }),
      ],
    });

    const s3Policy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ['arn:aws:s3:::'+vehicleEventFlinkApplicationAsset.s3BucketName+'/',
            'arn:aws:s3:::'+vehicleEventFlinkApplicationAsset.s3BucketName+'/'+vehicleEventFlinkApplicationAsset.s3ObjectKey,
            'arn:aws:s3:::'+preProcessorFlinkApplicationAsset.s3BucketName+'/',
            'arn:aws:s3:::'+preProcessorFlinkApplicationAsset.s3BucketName+'/'+preProcessorFlinkApplicationAsset.s3ObjectKey],
          actions: ['s3:ListBucket','s3:GetBucketLocation','s3:GetObject','s3:GetObjectVersion']
        }),
      ],
    });

    const lambdaCWPolicy = new iam.PolicyDocument({
      statements: [
        new iam.PolicyStatement({
          resources: ["arn:aws:logs:" + this.region + ":" + this.account+":*",
            "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:/aws/lambda/createTopicLambdaFunction:*"],
          actions: ['logs:PutLogEvents','logs:DescribeLogGroups','logs:DescribeLogStreams','logs:CreateLogGroup','logs:CreateLogStream'],
        }),
      ],
    });


    const lambdaMSKRole = new iam.Role(this, 'Lambda MSK Role', {
      assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
      roleName: this.stackName+"lambda-role",
      description: 'Lambda Role',
      inlinePolicies: {
        VPCPolicy: accessVPCPolicy,
        LogsPolicy: lambdaCWPolicy,
      },
    });

    const vehicleManagedFlinkRole = new iam.Role(this, 'Vehicle Managed Flink Role', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      roleName: this.stackName+"-flink-role",
      description: 'Managed Flink BedRock Role',
      inlinePolicies: {
        KDAAccessPolicy: kdaAccessPolicy,
        AccessCWLogsPolicy: accessCWLogsPolicy,
        S3Policy: s3Policy,
        VPCPolicy: accessVPCPolicy
      },
    });

    const preProcessormanagedFlinkRole = new iam.Role(this, 'PreProcessor Managed Flink Role', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      roleName: this.stackName+"preprocessor-flink-role",
      description: 'Managed Flink BedRock Role',
      inlinePolicies: {
        KDAAccessPolicy: kdaAccessPolicy,
        AccessCWLogsPolicy: preProcessorAccessCWLogsPolicy,
        S3Policy: s3Policy,
        VPCPolicy: accessVPCPolicy,
        PrometheusPolicy: remoteWritePolicy
      },
    });

    // instantiate serverless MSK cluster w/ IAM auth
    const MSKCluster = new MSKContruct(this, 'MSKCluster', {
      account: this.account,
      region: this.region,
      vpc: vpc,
      clusterName: this.stackName,
      kafkaVersion: '3.6.0',
      instanceType: 'kafka.m7g.large',
      mskSG: mskSG,
      sshSG: mskSG,
    });

    const createTopic = new lambda.Function(this, "createTopic", {
      runtime: lambda.Runtime.PYTHON_3_11,
      functionName: 'createTopicLambdaFunction',
      code: lambda.Code.fromAsset("./topicCreation"),
      vpc:vpc,
      securityGroups:[mskSG],
      vpcSubnets: {subnetType: SubnetType.PRIVATE_WITH_EGRESS},
      role: lambdaMSKRole,
      handler: "index.on_event",
      timeout: cdk.Duration.minutes(5),
      memorySize: 256
    })

    createTopic.addLayers(kafkaClientLayer)

    const createTopicProvider = new Provider(this, "createTopicProvider", {
      onEventHandler: createTopic,
      logRetention: aws_logs.RetentionDays.ONE_WEEK
    })

    const createTopicResource = new CustomResource(this, "createTopicResource", {
      serviceToken: createTopicProvider.serviceToken,
      properties: {
        topic_name: 'vehicle-events',
        bootstrap_servers: MSKCluster.bootstrapServersOutput.value,
        num_partitions: 64,
        replication_factor:2
      }
    })

    createTopicResource.node.addDependency(MSKCluster);

    const vehicleFlinkApplication = new kda.CfnApplication(this, 'vehicleFlinkApplication', {
      applicationName: 'vehicle-flink-application',
      runtimeEnvironment: 'FLINK-1_20',
      serviceExecutionRole: vehicleManagedFlinkRole.roleArn,
      applicationConfiguration: {
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: 'arn:aws:s3:::'+vehicleEventFlinkApplicationAsset.s3BucketName,
              fileKey: vehicleEventFlinkApplicationAsset.s3ObjectKey
            }
          },
          codeContentType: "ZIPFILE"
        },
        applicationSnapshotConfiguration: {
          snapshotsEnabled: true
        },
        vpcConfigurations: [ {
          subnetIds: vpc.selectSubnets({
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          }).subnetIds,
          securityGroupIds: [mskSG.securityGroupId]
        }
        ],
        environmentProperties: {
          propertyGroups: [{
            propertyGroupId: 'KafkaSink',
            propertyMap: {
              'bootstrap.servers': MSKCluster.bootstrapServersOutput.value,
              'topic': 'vehicle-events'
            },

          },
            {
              propertyGroupId: 'DataGen',
              propertyMap: {
                'vehicles': "100000",
                'events.per.sec': '100000',
                'prob.motion.state.change': "0.01",
                'prob.warning.change': '0.001',
              },
            },

          ],
        },


        flinkApplicationConfiguration: {
          parallelismConfiguration: {
            parallelism: 64,
            configurationType: 'CUSTOM',
            parallelismPerKpu: 8,
            autoScalingEnabled: false

          },
          monitoringConfiguration: {
            configurationType: "CUSTOM",
            metricsLevel: "APPLICATION",
            logLevel: "INFO"
          },
        }

      }
    });

    vehicleFlinkApplication.node.addDependency(MSKCluster);

    const cfnApplicationCloudWatchLoggingOption= new kda.CfnApplicationCloudWatchLoggingOption(this,"managedFlinkLogs", {
      applicationName: 'vehicle-flink-application',
      cloudWatchLoggingOption: {
        logStreamArn: "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+vehicleLogGroup.logGroupName+":log-stream:" + vehicleLogStream.logStreamName,
      },
    });

    cfnApplicationCloudWatchLoggingOption.node.addDependency(vehicleFlinkApplication);


    const startFlinkApplicationHandler = new lambda.Function(this, "startFlinkApplicationHandler", {
      runtime: lambda.Runtime.PYTHON_3_12,
      code: lambda.Code.fromAsset("./startFlinkApplication"),
      handler: "index.on_event",
      timeout: cdk.Duration.minutes(14),
      memorySize: 512
    })

    const startFlinkApplicationProvider = new Provider(this, "startFlinkApplicationProvider", {
      onEventHandler: startFlinkApplicationHandler,
      logRetention: aws_logs.RetentionDays.ONE_WEEK
    })

    startFlinkApplicationHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "kinesisanalytics:DescribeApplication",
        "kinesisanalytics:StartApplication",
        "kinesisanalytics:StopApplication",

      ],
      resources: [`arn:aws:kinesisanalytics:${this.region}:${this.account}:application/`+vehicleFlinkApplication.applicationName]
    }))

    const startFlinkApplicationResource = new CustomResource(this, "startFlinkApplicationResource", {
      serviceToken: startFlinkApplicationProvider.serviceToken,
      properties: {
        AppName: vehicleFlinkApplication.applicationName,
      }
    })

    startFlinkApplicationResource.node.addDependency(vehicleFlinkApplication);
    startFlinkApplicationResource.node.addDependency(createTopicResource);

    const preProcessorFlinkApplication = new kda.CfnApplication(this, 'preProcessorFlinkApplication', {
      applicationName: 'preProcessor-flink-application',
      runtimeEnvironment: 'FLINK-1_20',
      serviceExecutionRole: preProcessormanagedFlinkRole.roleArn,
      applicationConfiguration: {
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: 'arn:aws:s3:::'+preProcessorFlinkApplicationAsset.s3BucketName,
              fileKey: preProcessorFlinkApplicationAsset.s3ObjectKey
            }
          },
          codeContentType: "ZIPFILE"
        },
        applicationSnapshotConfiguration: {
          snapshotsEnabled: true
        },
        vpcConfigurations: [ {
          subnetIds: vpc.selectSubnets({
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
          }).subnetIds,
          securityGroupIds: [mskSG.securityGroupId]
        }
        ],
        environmentProperties: {
          propertyGroups: [{
            propertyGroupId: 'KafkaSource',
            propertyMap: {
              'bootstrap.servers': MSKCluster.bootstrapServersOutput.value,
              'topic': 'vehicle-events'
            },

          },
            {
              propertyGroupId: 'PrometheusSink',
              propertyMap: {
                'endpoint.url': prometheusWorkspace.attrPrometheusEndpoint+'api/v1/remote_write'
              },
            },
            {
              propertyGroupId: 'Aggregation',
              propertyMap: {
                'window.size.sec': "5"
              },
            },
          ],
        },

        flinkApplicationConfiguration: {
          parallelismConfiguration: {
            parallelism: 64,
            configurationType: 'CUSTOM',
            parallelismPerKpu: 8,
            autoScalingEnabled: false

          },
          monitoringConfiguration: {
            configurationType: "CUSTOM",
            metricsLevel: "APPLICATION",
            logLevel: "INFO"
          },
        }

      }
    });

    preProcessorFlinkApplication.node.addDependency(MSKCluster);


    const cfnApplicationCloudWatchLoggingOptionPreProcessor= new kda.CfnApplicationCloudWatchLoggingOption(this,"managedFlinkLogsPreProcessor", {
      applicationName: 'preProcessor-flink-application',
      cloudWatchLoggingOption: {
        logStreamArn: "arn:aws:logs:" + this.region + ":" + this.account + ":log-group:"+preProcessorlogGroup.logGroupName+":log-stream:" + preProcessorlogStream.logStreamName,
      },
    });

    cfnApplicationCloudWatchLoggingOptionPreProcessor.node.addDependency(preProcessorFlinkApplication)


    startFlinkApplicationHandler.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        "kinesisanalytics:DescribeApplication",
        "kinesisanalytics:StartApplication",
        "kinesisanalytics:StopApplication",

      ],
      resources: [`arn:aws:kinesisanalytics:${this.region}:${this.account}:application/`+preProcessorFlinkApplication.applicationName]
    }))

    const startPreProcessorFlinkApplicationResource = new CustomResource(this, "startPreProcessorFlinkApplicationResource", {
      serviceToken: startFlinkApplicationProvider.serviceToken,
      properties: {
        AppName: preProcessorFlinkApplication.applicationName,
      }
    })

    startPreProcessorFlinkApplicationResource.node.addDependency(preProcessorFlinkApplication);
    startPreProcessorFlinkApplicationResource.node.addDependency(startFlinkApplicationResource);

  }
}


