using System.Reactive.Subjects;
using AutoMuteUsPortable.Shared.Controller.Executor;
using AutoMuteUsPortable.Shared.Entity.ProgressInfo;

namespace AutoMuteUsPortable.Executor;

public class ExecutorController : IExecutorController
{
    public static Dictionary<string, Parameter> InstallParameters = new();
    public static Dictionary<string, Parameter> UpdateParameters = new();

    public ExecutorController(dynamic executorConfiguration)
    {
        throw new NotImplementedException();
    }

    public ExecutorController(dynamic computedSimpleSettings,
        dynamic executorConfigurationBase)
    {
        throw new NotImplementedException();
    }

    public async Task Run(ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public async Task Stop(ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public async Task Restart(ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public async Task Install(Dictionary<string, string> parameters, ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public async Task Update(Dictionary<string, string> parameters, ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public async Task InstallBySimpleSettings(dynamic simpleSettings, dynamic executorConfigurationBase,
        ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }

    public async Task UpdateBySimpleSettings(dynamic simpleSettings, dynamic executorConfigurationBase,
        ISubject<ProgressInfo>? progress = null)
    {
        throw new NotImplementedException();
    }
}