// <copyright file="AWSECSResourceDetector.cs" company="OpenTelemetry Authors">
// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.RegularExpressions;

using Newtonsoft.Json.Linq;

using OpenTelemetry.Resources;

namespace OpenTelemetry.Contrib.Extensions.AWSXRay.Resources;

/// <summary>
/// Resource detector for application running in AWS ECS.
/// </summary>
public class AWSECSResourceDetector : IResourceDetector
{
    private const string AWSECSMetadataPath = "/proc/self/cgroup";
    private const string AWSECSMetadataURLKey = "ECS_CONTAINER_METADATA_URI";
    private const string AWSECSMetadataURLV4Key = "ECS_CONTAINER_METADATA_URI_V4";

    /// <summary>
    /// Detector the required and optional resource attributes from AWS ECS.
    /// </summary>
    /// <returns>Resource.</returns>
    public Resource Detect()
    {
        if (!this.IsECSProcess())
        {
            return null;
        }

        var resourceAttributes = new List<KeyValuePair<string, object>>()
        {
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeCloudProvider, "aws"),
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeCloudPlatform, "aws_ecs"),
        };

        try
        {
            var containerId = this.GetECSContainerId(AWSECSMetadataPath);
            resourceAttributes.AddRange(ExtractResourceAttributes(containerId));
        }
        catch (Exception ex)
        {
            AWSXRayEventSource.Log.ResourceAttributesExtractException(nameof(AWSECSResourceDetector), ex);
        }

        try
        {
            resourceAttributes.AddRange(ExtractMetadataV4ResourceAttributes());
        }
        catch (Exception ex)
        {
            AWSXRayEventSource.Log.ResourceAttributesExtractException(nameof(AWSECSResourceDetector), ex);
        }

        return new Resource(resourceAttributes);
    }

    internal static string GetStringOrThrow(JObject obj, string fieldName)
    {
        var value = obj[fieldName]?.ToString();
        if (value == null)
        {
            throw new ArgumentNullException($"The expected '{fieldName}' field is missing");
        }

        return value;
    }

    internal static List<KeyValuePair<string, object>> ExtractResourceAttributes(string containerId)
    {
        var resourceAttributes = new List<KeyValuePair<string, object>>()
        {
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeContainerID, containerId),
        };

        return resourceAttributes;
    }

    internal static List<KeyValuePair<string, object>> ExtractMetadataV4ResourceAttributes()
    {
        var metadataV4Url = Environment.GetEnvironmentVariable(AWSECSMetadataURLV4Key);
        if (metadataV4Url == null)
        {
            return new List<KeyValuePair<string, object>>();
        }

        var httpClientHandler = new HttpClientHandler();
        var metadataV4ContainerResponse = ResourceDetectorUtils.SendOutRequest(metadataV4Url, "GET", null, httpClientHandler).Result;
        var metadataV4TaskResponse = ResourceDetectorUtils.SendOutRequest($"{metadataV4Url.TrimEnd('/')}/task", "GET", null, httpClientHandler).Result;

        var containerResponse = JObject.Parse(metadataV4ContainerResponse);
        var taskResponse = JObject.Parse(metadataV4TaskResponse);

        var containerArn = GetStringOrThrow(containerResponse, "ContainerARN");
        var clusterArn = GetStringOrThrow(taskResponse, "Cluster");

        if (!clusterArn.StartsWith("arn:"))
        {
            var baseArn = containerArn.Substring(containerArn.LastIndexOf(":"));
            clusterArn = $"{baseArn}:cluster/{clusterArn}";
        }

        var launchType = GetStringOrThrow(taskResponse, "LaunchType") switch
        {
            string type when "ec2".Equals(type.ToLower()) => AWSSemanticConventions.ValueEcsLaunchTypeEc2,
            string type when "fargate".Equals(type.ToLower()) => AWSSemanticConventions.ValueEcsLaunchTypeFargate,
            _ => null,
        };

        if (launchType == null)
        {
            throw new ArgumentOutOfRangeException($"Unrecognized launch type '{taskResponse["LaunchType"]}'");
        }

        var resourceAttributes = new List<KeyValuePair<string, object>>()
        {
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeEcsContainerArn, containerArn),
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeEcsClusterArn, clusterArn),
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeEcsLaunchtype, launchType),
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeEcsTaskArn, (string)taskResponse["TaskARN"]),
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeEcsTaskFamily, (string)taskResponse["Family"]),
            new KeyValuePair<string, object>(AWSSemanticConventions.AttributeEcsTaskRevision, (string)taskResponse["Revision"]),
        };

        if ("awslogs".Equals(GetStringOrThrow(containerResponse, "LogDriver")))
        {
            JObject logOptions = (JObject)containerResponse["LogOptions"];
            if (logOptions == null)
            {
                throw new ArgumentNullException("The expected 'LogOptions' object is missing");
            }

            var regex = new Regex(@"arn:aws:ecs:([^:]+):([^:]+):.*");
            var match = regex.Match(containerArn);

            if (!match.Success)
            {
                throw new ArgumentOutOfRangeException($"Cannot parse region and account from the container ARN '{containerArn}'");
            }

            var logsRegion = match.Groups[1];
            var logsAccount = match.Groups[2];

            var logGroupName = GetStringOrThrow(logOptions, "awslogs-group");
            var logStreamName = GetStringOrThrow(logOptions, "awslogs-stream");

            resourceAttributes.Add(new KeyValuePair<string, object>(AWSSemanticConventions.AttributeLogGroupNames, new string[] { logGroupName }));
            resourceAttributes.Add(new KeyValuePair<string, object>(AWSSemanticConventions.AttributeLogGroupArns, new string[] { $"arn:aws:logs:{logsRegion}:{logsAccount}:log-group:{logGroupName}:*" }));
            resourceAttributes.Add(new KeyValuePair<string, object>(AWSSemanticConventions.AttributeLogStreamNames, new string[] { logStreamName }));
            resourceAttributes.Add(new KeyValuePair<string, object>(AWSSemanticConventions.AttributeLogStreamArns, new string[] { $"arn:aws:logs:{logsRegion}:{logsAccount}:log-group:{logGroupName}:log-stream:{logStreamName}" }));
        }

        return resourceAttributes;
    }

    internal string GetECSContainerId(string path)
    {
        string containerId = null;

        using (var streamReader = ResourceDetectorUtils.GetStreamReader(path))
        {
            while (!streamReader.EndOfStream)
            {
                var trimmedLine = streamReader.ReadLine().Trim();
                if (trimmedLine.Length > 64)
                {
                    containerId = trimmedLine.Substring(trimmedLine.Length - 64);
                    return containerId;
                }
            }
        }

        return containerId;
    }

    internal bool IsECSProcess()
    {
        return Environment.GetEnvironmentVariable(AWSECSMetadataURLKey) != null || Environment.GetEnvironmentVariable(AWSECSMetadataURLV4Key) != null;
    }
}
