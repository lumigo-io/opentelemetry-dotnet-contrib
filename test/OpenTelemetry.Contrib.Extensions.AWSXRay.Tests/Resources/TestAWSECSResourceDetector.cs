// <copyright file="TestAWSECSResourceDetector.cs" company="OpenTelemetry Authors">
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
using System.Linq;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using OpenTelemetry.Contrib.Extensions.AWSXRay.Resources;
using Xunit;

namespace OpenTelemetry.Contrib.Extensions.AWSXRay.Tests.Resources;

public class TestAWSECSResourceDetector
{
    private const string AWSECSMetadataFilePath = "Resources/SampleMetadataFiles/testcgroup";
    private const string AWSECSMetadataURLKey = "ECS_CONTAINER_METADATA_URI";
    private const string AWSECSMetadataURLV4Key = "ECS_CONTAINER_METADATA_URI_V4";

    [Fact]
    public void TestDetect()
    {
        IEnumerable<KeyValuePair<string, object>> resourceAttributes;
        var ecsResourceDetector = new AWSECSResourceDetector();
        resourceAttributes = ecsResourceDetector.Detect();
        Assert.Null(resourceAttributes); // will be null as it's not in ecs environment
    }

    [Fact]
    public void TestExtractResourceAttributes()
    {
        var ecsResourceDetector = new AWSECSResourceDetector();
        var containerId = "Test container id";

        var resourceAttributes = ecsResourceDetector.ExtractResourceAttributes(containerId).ToDictionary(x => x.Key, x => x.Value);

        Assert.Equal("aws", resourceAttributes[AWSSemanticConventions.AttributeCloudProvider]);
        Assert.Equal("aws_ecs", resourceAttributes[AWSSemanticConventions.AttributeCloudPlatform]);
        Assert.Equal("Test container id", resourceAttributes[AWSSemanticConventions.AttributeContainerID]);
    }

    [Fact]
    public void TestGetECSContainerId()
    {
        var ecsResourceDetector = new AWSECSResourceDetector();
        var ecsContainerId = ecsResourceDetector.GetECSContainerId(AWSECSMetadataFilePath);

        Assert.Equal("a4d00c9dd675d67f866c786181419e1b44832d4696780152e61afd44a3e02856", ecsContainerId);
    }

    [Fact]
    public void TestIsECSProcess()
    {
        Environment.SetEnvironmentVariable(AWSECSMetadataURLKey, "TestECSURIKey");

        var ecsResourceDetector = new AWSECSResourceDetector();
        var isEcsProcess = ecsResourceDetector.IsECSProcess();

        Assert.True(isEcsProcess);
    }

    [Fact]
    public async void TestIsECSProcessV4Fargate()
    {
        CancellationTokenSource source = new CancellationTokenSource();
        CancellationToken token = source.Token;

        var port = 8912; // TODO Change to random open

        try
        {
            using var server = new WebHostBuilder()
                .UseKestrel(/*x => x.ListenLocalhost(8080)*/).UseStartup<Startup>()
                .UseUrls($"http://localhost:{port}")
                .Configure(app =>
            {
                app.Run(async context =>
                {
                    if (context.Request.Method == HttpMethods.Get && context.Request.Path == "/")
                    {
                        string content = await ReadTextAsync($"{System.IO.Path.GetDirectoryName(Application.ExecutablePath)}/Resources/ecs_metadata/metadatav4-response-container-fargate.json");
                        await context.Response.WriteAsJsonAsync(content);
                    }
                    else if (context.Request.Method == HttpMethods.Post && context.Request.Path == "/task")
                    {
                        string content = await ReadTextAsync($"{System.IO.Path.GetDirectoryName(Application.ExecutablePath)}/Resources/ecs_metadata/metadatav4-response-task-fargate.json");
                        await context.Response.WriteAsJsonAsync(storage);
                    }
                    else
                    {
                        context.Response.StatusCode = StatusCodes.Status404NotFound;
                        await context.Response.WriteAsync("Not found");
                    }
                });
            }).Build();
            await server.StartAsync();

            try
            {
                Environment.SetEnvironmentVariable(AWSECSMetadataURLV4Key, $"http://localhost:{port}");

                var ecsResourceDetector = new AWSECSResourceDetector();

                Assert.True(ecsResourceDetector.IsECSProcess());
            }
            finally
            {
                await server.StopAsync(token);
            }
        }
        finally
        {
            source.Dispose();
        }
    }

    [Fact]
    public void TestIsNotECSProcess()
    {
        var ecsResourceDetector = new AWSECSResourceDetector();
        var isEcsProcess = ecsResourceDetector.IsECSProcess();

        Assert.False(isEcsProcess);
    }
}
