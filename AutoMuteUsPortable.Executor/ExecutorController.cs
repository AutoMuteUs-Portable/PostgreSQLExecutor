using System.Diagnostics;
using System.Management;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using AutoMuteUsPortable.PocketBaseClient;
using AutoMuteUsPortable.Shared.Controller.Executor;
using AutoMuteUsPortable.Shared.Entity.ExecutorConfigurationBaseNS;
using AutoMuteUsPortable.Shared.Entity.ExecutorConfigurationNS;
using AutoMuteUsPortable.Shared.Entity.ProgressInfo;
using AutoMuteUsPortable.Shared.Utility;
using CliWrap;
using FluentValidation;
using Serilog;

namespace AutoMuteUsPortable.Executor;

public class ExecutorController : ExecutorControllerBase
{
    private readonly PocketBaseClientApplication _pocketBaseClientApplication = new();
    private IDisposable _healthChecker = Disposable.Empty;

    public ExecutorController(object executorConfiguration) : base(executorConfiguration)
    {
        #region Check variables

        var binaryDirectory = Utils.PropertyByName<string>(executorConfiguration, "binaryDirectory");
        if (binaryDirectory == null)
            throw new InvalidDataException("binaryDirectory cannot be null");

        var binaryVersion = Utils.PropertyByName<string>(executorConfiguration, "binaryVersion");
        if (binaryVersion == null)
            throw new InvalidDataException("binaryVersion cannot be null");

        var version = Utils.PropertyByName<string>(executorConfiguration, "version");
        if (version == null) throw new InvalidDataException("version cannot be null");

        ExecutorType? type = Utils.PropertyByName<ExecutorType>(executorConfiguration, "type");
        if (type == null) throw new InvalidDataException("type cannot be null");

        var environmentVariables =
            Utils.PropertyByName<Dictionary<string, string>>(executorConfiguration, "environmentVariables");
        if (environmentVariables == null) throw new InvalidDataException("environmentVariables cannot be null");

        #endregion

        #region Create ExecutorConfiguration and validate

        ExecutorConfiguration tmp = new()
        {
            version = version,
            type = (ExecutorType)type,
            binaryVersion = binaryVersion,
            binaryDirectory = binaryDirectory,
            environmentVariables = environmentVariables
        };

        var validator = new ExecutorConfigurationValidator();
        validator.ValidateAndThrow(tmp);

        ExecutorConfiguration = tmp;

        Log.Debug("ExecutorController is instantiated with {@ExecutorConfiguration}", ExecutorConfiguration);

        #endregion
    }

    public ExecutorController(object computedSimpleSettings,
        object executorConfigurationBase) : base(computedSimpleSettings, executorConfigurationBase)
    {
        #region Check variables

        var binaryDirectory = Utils.PropertyByName<string>(executorConfigurationBase, "binaryDirectory");
        if (binaryDirectory == null)
            throw new InvalidDataException("binaryDirectory cannot be null");

        var binaryVersion = Utils.PropertyByName<string>(executorConfigurationBase, "binaryVersion");
        if (binaryVersion == null)
            throw new InvalidDataException("binaryVersion cannot be null");

        var version = Utils.PropertyByName<string>(executorConfigurationBase, "version");
        if (version == null) throw new InvalidDataException("version cannot be null");

        ExecutorType? type = Utils.PropertyByName<ExecutorType>(executorConfigurationBase, "type");
        if (type == null) throw new InvalidDataException("type cannot be null");

        if (Utils.PropertyInfoByName(computedSimpleSettings, "port") == null)
            throw new InvalidDataException("port is not found in computedSimpleSettings");
        var port = Utils.PropertyByName<object>(computedSimpleSettings, "port");
        int? postgresqlPort = Utils.PropertyByName<int>(port!, "postgresql");
        if (postgresqlPort == null) throw new InvalidDataException("postgresqlPort cannot be null");

        var postgresqlConfiguration = Utils.PropertyByName<object>(computedSimpleSettings, "postgresql");
        if (postgresqlConfiguration == null)
            throw new InvalidDataException("postgresqlConfiguration cannot be null");

        var postgresqlUsername = Utils.PropertyByName<string>(postgresqlConfiguration, "username");
        if (string.IsNullOrEmpty(postgresqlUsername))
            throw new InvalidDataException("postgresqlUsername cannot be null or empty");

        var postgresqlPassword = Utils.PropertyByName<string>(postgresqlConfiguration, "password");
        if (string.IsNullOrEmpty(postgresqlPassword))
            throw new InvalidDataException("postgresqlPassword cannot be null or empty");

        #endregion

        #region Create ExecutorConfiguration and validate

        ExecutorConfiguration executorConfiguration = new()
        {
            version = version,
            type = (ExecutorType)type,
            binaryVersion = binaryVersion,
            binaryDirectory = binaryDirectory,
            environmentVariables = new Dictionary<string, string>
            {
                ["POSTGRESQL_PORT"] = postgresqlPort.ToString() ?? "",
                ["POSTGRESQL_USERNAME"] = postgresqlUsername,
                ["POSTGRESQL_PASSWORD"] = postgresqlPassword
            }
        };

        var validator = new ExecutorConfigurationValidator();
        validator.ValidateAndThrow(executorConfiguration);

        ExecutorConfiguration = executorConfiguration;

        Log.Debug("ExecutorController is instantiated with {@ExecutorConfiguration}", ExecutorConfiguration);

        #endregion
    }

    private IDisposable HealthChecker
    {
        get => _healthChecker;
        set
        {
            _healthChecker.Dispose();
            _healthChecker = value;
        }
    }

    public override async Task Run(ISubject<ProgressInfo>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (IsRunning) return;

        #region Setup progress

        var taskProgress = progress != null
            ? new TaskProgress(progress, new Dictionary<string, object?>
            {
                ["File integrity check"] = new List<string>
                {
                    "Checking file integrity",
                    "Downloading",
                    "Extracting"
                },
                ["Killing currently running server"] = null,
                ["Starting server"] = null
            })
            : null;

        #endregion

        #region Retrieve data from PocketBase

        var postgresql =
            _pocketBaseClientApplication.Data.PostgresqlCollection.FirstOrDefault(x =>
                x.Version == ExecutorConfiguration.binaryVersion);
        if (postgresql == null)
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not found in the database");
        if (postgresql.CompatibleExecutors.All(x => x.Version != ExecutorConfiguration.version))
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not compatible with Executor {ExecutorConfiguration.version}");

        #endregion

        #region Check file integrity

        var checksumUrl = Utils.GetChecksum(postgresql.Checksum);

        if (string.IsNullOrEmpty(checksumUrl))
        {
#if DEBUG
            Log.Debug("Checksum is null or empty, skipping integrity check");
            taskProgress?.NextTask(3);
#else
                throw new InvalidDataException("Checksum cannot be null or empty");
#endif
        }
        else
        {
            using (var client = new HttpClient())
            {
                var res = await client.GetStringAsync(checksumUrl, cancellationToken);
                var checksum = Utils.ParseChecksumText(res);
                var checksumProgress = taskProgress?.GetSubjectProgress();
                checksumProgress?.OnNext(new ProgressInfo
                {
                    name = string.Format("{0}のファイルの整合性を確認しています", ExecutorConfiguration.type),
                    IsIndeterminate = true
                });
                var invalidFiles =
                    Utils.CompareChecksum(ExecutorConfiguration.binaryDirectory, checksum, cancellationToken);
                taskProgress?.NextTask();

                if (0 < invalidFiles.Count)
                {
                    var downloadUrl = Utils.GetDownloadUrl(postgresql.DownloadUrl);
                    if (string.IsNullOrEmpty(downloadUrl))
                        throw new InvalidDataException("DownloadUrl cannot be null or empty");

                    var binaryPath = Path.Combine(ExecutorConfiguration.binaryDirectory,
                        Path.GetFileName(downloadUrl));

                    var downloadProgress = taskProgress?.GetProgress();
                    if (taskProgress?.ActiveLeafTask != null)
                        taskProgress.ActiveLeafTask.Name =
                            string.Format("{0}の実行に必要なファイルをダウンロードしています", ExecutorConfiguration.type);
                    await Utils.DownloadAsync(downloadUrl, binaryPath, downloadProgress, cancellationToken);
                    taskProgress?.NextTask();

                    var extractProgress = taskProgress?.GetProgress();
                    if (taskProgress?.ActiveLeafTask != null)
                        taskProgress.ActiveLeafTask.Name =
                            string.Format("{0}の実行に必要なファイルを解凍しています", ExecutorConfiguration.type);
                    Utils.ExtractZip(binaryPath, extractProgress, cancellationToken);
                    taskProgress?.NextTask();
                }
                else
                {
                    taskProgress?.NextTask(2);
                }
            }
        }

        #endregion

        #region Search for currently running process and kill it

        var fileName = Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\postgres.exe");

        var killingProgress = taskProgress?.GetSubjectProgress();
        killingProgress?.OnNext(new ProgressInfo
        {
            name = string.Format("既に起動している{0}を終了しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });
        var wmiQueryString =
            $"SELECT ProcessId FROM Win32_Process WHERE ExecutablePath = '{fileName.Replace(@"\", @"\\")}'";
        using (var searcher = new ManagementObjectSearcher(wmiQueryString))
        using (var results = searcher.Get())
        {
            foreach (var result in results)
            {
                try
                {
                    Log.Debug("Killing already running process {ProcessId}", result["ProcessId"]);
                    var processId = (uint)result["ProcessId"];
                    var process = Process.GetProcessById((int)processId);

                    process.Kill();
                }
                catch
                {
                    // ignored
                }

                cancellationToken.ThrowIfCancellationRequested();
            }
        }

        taskProgress?.NextTask();

        #endregion

        #region Generate config

        var dataDirectory = Path.Combine(ExecutorConfiguration.binaryDirectory, @"data\");
        var defaultPostgresqlConfPath = Path.Combine(dataDirectory, "postgresql.default.conf");
        var postgresqlConf = await File.ReadAllTextAsync(defaultPostgresqlConfPath, cancellationToken);
        var postgresqlConfPath = Path.Combine(dataDirectory, "postgresql.conf");
        postgresqlConf = postgresqlConf.Replace("#port = 5432",
            $"port = {ExecutorConfiguration.environmentVariables["POSTGRESQL_PORT"]}");

        await File.WriteAllTextAsync(postgresqlConfPath, postgresqlConf, cancellationToken);

        #endregion

        #region Start server

        var startProgress = taskProgress?.GetSubjectProgress();
        startProgress?.OnNext(new ProgressInfo
        {
            name = string.Format("{0}を起動しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });
        cancellationToken.Register(() => ForciblyStop());
        Cli.Wrap(Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\pg_ctl.exe"))
            .WithArguments($"start -w -D \"{dataDirectory.Replace(@"\", @"\\")}\"")
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError))
            .WithValidation(CommandResultValidation.None)
            .ExecuteAsync();

        CreateHealthChecker();

        taskProgress?.NextTask();

        #endregion
    }

    public override async Task GracefullyStop(ISubject<ProgressInfo>? progress = null,
        CancellationToken cancellationToken = default)
    {
        if (!IsRunning) return;

        Log.Debug("Gracefully stopping {Type}", ExecutorConfiguration.type);

        #region Stop server in postgresql manner

        progress?.OnNext(new ProgressInfo
        {
            name = string.Format("{0}を終了しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });

        HealthChecker.Dispose();

        await Cli.Wrap(Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\pg_ctl.exe"))
            .WithArguments(
                $"stop -D \"{Path.Combine(ExecutorConfiguration.binaryDirectory, @"data\").Replace(@"\", @"\\")}\" -m fast")
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError))
            .WithValidation(CommandResultValidation.None)
            .ExecuteAsync(cancellationToken);

        OnStop();

        #endregion
    }

    public override Task ForciblyStop(ISubject<ProgressInfo>? progress = null)
    {
        if (!IsRunning) return Task.CompletedTask;

        Log.Debug("Forcibly stopping {Type}", ExecutorConfiguration.type);

        #region Stop server in postgresql manner

        progress?.OnNext(new ProgressInfo
        {
            name = string.Format("{0}を終了しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });

        HealthChecker.Dispose();

        var fileName = Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\postgres.exe");
        var wmiQueryString =
            $"SELECT ProcessId FROM Win32_Process WHERE ExecutablePath = '{fileName.Replace(@"\", @"\\")}'";
        using (var searcher = new ManagementObjectSearcher(wmiQueryString))
        using (var results = searcher.Get())
        {
            foreach (var result in results)
                try
                {
                    var processId = (uint)result["ProcessId"];
                    var process = Process.GetProcessById((int)processId);

                    process.Kill();
                }
                catch
                {
                    // ignored
                }
        }

        OnStop();

        return Task.CompletedTask;

        #endregion
    }

    public override Task Restart(ISubject<ProgressInfo>? progress = null, CancellationToken cancellationToken = default)
    {
        if (!IsRunning) return Task.CompletedTask;

        #region Restart server in postgresql manner

        progress?.OnNext(new ProgressInfo
        {
            name = string.Format("{0}を再起動しています", ExecutorConfiguration.type),
            IsIndeterminate = true
        });

        cancellationToken.Register(() => ForciblyStop());
        Cli.Wrap(Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\pg_ctl.exe"))
            .WithArguments(
                $"restart -w -D \"{Path.Combine(ExecutorConfiguration.binaryDirectory, @"data\").Replace(@"\", @"\\")}\" -m fast")
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError))
            .WithValidation(CommandResultValidation.None)
            .ExecuteAsync();

        CreateHealthChecker();

        return Task.CompletedTask;

        #endregion
    }

    public override async Task Install(
        Dictionary<ExecutorType, ExecutorControllerBase> executors, ISubject<ProgressInfo>? progress = null,
        CancellationToken cancellationToken = default)
    {
        #region Setup progress

        var taskProgress = progress != null
            ? new TaskProgress(progress, new List<string>
            {
                "Downloading",
                "Extracting",
                "Initializing"
            })
            : null;

        #endregion

        #region Retrieve data from PocketBase

        var postgresql =
            _pocketBaseClientApplication.Data.PostgresqlCollection.FirstOrDefault(x =>
                x.Version == ExecutorConfiguration.binaryVersion);
        if (postgresql == null)
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not found in the database");
        if (postgresql.CompatibleExecutors.All(x => x.Version != ExecutorConfiguration.version))
            throw new InvalidDataException(
                $"{ExecutorConfiguration.type} {ExecutorConfiguration.binaryVersion} is not compatible with Executor {ExecutorConfiguration.version}");
        var downloadUrl = Utils.GetDownloadUrl(postgresql.DownloadUrl);
        if (string.IsNullOrEmpty(downloadUrl))
            throw new InvalidDataException("DownloadUrl cannot be null or empty");

        #endregion

        #region Download

        if (!Directory.Exists(ExecutorConfiguration.binaryDirectory))
            Directory.CreateDirectory(ExecutorConfiguration.binaryDirectory);

        var binaryPath = Path.Combine(ExecutorConfiguration.binaryDirectory,
            Path.GetFileName(downloadUrl));

        var downloadProgress = taskProgress?.GetProgress();
        if (taskProgress?.ActiveLeafTask != null)
            taskProgress.ActiveLeafTask.Name = string.Format("{0}の実行に必要なファイルをダウンロードしています", ExecutorConfiguration.type);
        await Utils.DownloadAsync(downloadUrl, binaryPath, downloadProgress, cancellationToken);
        taskProgress?.NextTask();

        #endregion

        #region Extract

        var extractProgress = taskProgress?.GetProgress();
        if (taskProgress?.ActiveLeafTask != null)
            taskProgress.ActiveLeafTask.Name = string.Format("{0}の実行に必要なファイルを解凍しています", ExecutorConfiguration.type);
        Utils.ExtractZip(binaryPath, extractProgress, cancellationToken);
        taskProgress?.NextTask();

        #endregion

        #region Initialize database

        var passwordFile = Path.GetTempFileName();
        await File.WriteAllTextAsync(passwordFile, ExecutorConfiguration.environmentVariables["POSTGRESQL_PASSWORD"],
            cancellationToken);

        var initializeProgress = taskProgress?.GetSubjectProgress();
        initializeProgress?.OnNext(new ProgressInfo
        {
            name = "データベースを初期化しています",
            IsIndeterminate = true
        });

        await Cli.Wrap(Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\initdb.exe"))
            .WithArguments(
                $"--locale=\"en_US.UTF-8\" --lc-collate=\"en_US.UTF-8\" --lc-ctype=\"en_US.UTF-8\" -E UTF8 --auth=password --pwfile=\"{passwordFile}\" --username={ExecutorConfiguration.environmentVariables["POSTGRESQL_USERNAME"]} \"{Path.Combine(ExecutorConfiguration.binaryDirectory, @"data\").Replace(@"\", @"\\")}\"")
            .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
            .WithStandardOutputPipe(PipeTarget.ToDelegate(ProcessStandardOutput))
            .WithStandardErrorPipe(PipeTarget.ToDelegate(ProcessStandardError))
            .WithValidation(CommandResultValidation.None)
            .ExecuteAsync(cancellationToken);

        File.Move(Path.Combine(ExecutorConfiguration.binaryDirectory, @"data\postgresql.conf"),
            Path.Combine(ExecutorConfiguration.binaryDirectory, @"data\postgresql.default.conf"));

        taskProgress?.NextTask();

        #endregion
    }

    public override Task Update(
        Dictionary<ExecutorType, ExecutorControllerBase> executors, object oldExecutorConfiguration,
        ISubject<ProgressInfo>? progress = null, CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    private void CreateHealthChecker()
    {
        HealthChecker = Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(10)).Select(_ =>
            Observable.FromAsync(async () =>
            {
                var result = await Cli
                    .Wrap(Path.Combine(ExecutorConfiguration.binaryDirectory, @"bin\pg_isready.exe"))
                    .WithArguments($"-p {ExecutorConfiguration.environmentVariables["POSTGRESQL_PORT"]}")
                    .WithWorkingDirectory(ExecutorConfiguration.binaryDirectory)
                    .WithValidation(CommandResultValidation.None)
                    .ExecuteAsync();
                return result.ExitCode;
            }, TaskPoolScheduler.Default).Catch<int, Exception>(ex =>
            {
                Log.Warning("Unexpected error happened while health checking PostgreSQL: {Message}", ex.Message);
                return Observable.Empty<int>();
            })).Concat().Subscribe(
            exitCode =>
            {
                if (exitCode is 0 or 1) OnStart();
                else OnStop();
            }, _ => OnStop());
    }

    private void ProcessStandardOutput(string text)
    {
        Log.Verbose("[{ExecutorType}] {Text}", ExecutorConfiguration.type, text);

        StandardOutput.OnNext(text);
    }

    private void ProcessStandardError(string text)
    {
        Log.Verbose("[{ExecutorType}] {Text}", ExecutorConfiguration.type, text);

        StandardError.OnNext(text);
    }

    protected override void OnStart()
    {
        if (IsRunning) return;
        base.OnStart();
    }

    protected override void OnStop()
    {
        if (!IsRunning) return;
        base.OnStop();
    }
}