"""Application configuration loaded from ``settings.yaml``."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import yaml

ROOT_DIR = Path(__file__).parent.parent.parent

@dataclass
class AppConfig:
	name: str = "ska-sdp-long-observation"
	environment: str = "local"


@dataclass
class StorageConfig:
	data_dir: str = "/data"
	storage_threshold_mb: int = 10240

@dataclass
class ObservationConfig:
	receive_interval_seconds: int = 60
	retry_attempts: int = 2
	retry_delay_seconds: int = 10


@dataclass
class ProcessingConfig:
	max_concurrency: int = 2
	command_timeout_seconds: int = 3600


@dataclass
class ContainerImageConfig:
	image: str
	command: list[str] = field(default_factory=list)


@dataclass
class ContainersConfig:
	runner: str = "docker"
	receive: ContainerImageConfig = field(
		default_factory=lambda: ContainerImageConfig("ska-sdp-receive:latest")
	)
	process: ContainerImageConfig = field(
		default_factory=lambda: ContainerImageConfig("ska-sdp-process:latest")
	)
	volume_name: str = "ska-sdp-data"
	mount_path: str = "/data"


@dataclass
class QualityGateConfig:
	enabled: bool = True
	outcomes: list[str] = field(default_factory=lambda: ["Continue", "Reprocess"])
	max_attempts: int = 2


@dataclass
class KubernetesConfig:
	namespace: str = "default"
	pvc_name: str = "ska-sdp-data"
	image_pull_policy: str = "IfNotPresent"
	active_deadline_seconds: int = 3600


@dataclass
class LoggingConfig:
	level: str = "INFO"


@dataclass
class Config:
	ROOT_DIR: Path = field(default_factory=lambda: ROOT_DIR)
	app: AppConfig = field(default_factory=AppConfig)
	storage: StorageConfig = field(default_factory=StorageConfig)
	observation: ObservationConfig = field(default_factory=ObservationConfig)
	processing: ProcessingConfig = field(default_factory=ProcessingConfig)
	containers: ContainersConfig = field(default_factory=ContainersConfig)
	quality_gate: QualityGateConfig = field(default_factory=QualityGateConfig)
	kubernetes: KubernetesConfig = field(default_factory=KubernetesConfig)
	logging: LoggingConfig = field(default_factory=LoggingConfig)


def _section(cls: type[Any], values: dict[str, Any] | None) -> Any:
	return cls(**(values or {}))


def load_config(path: str | Path | None = None) -> Config:
	"""Load settings from the supplied YAML path or the adjacent settings file."""
	settings_path = Path(path) if path else Path(__file__).with_name("settings.yaml")
	with settings_path.open(encoding="utf-8") as stream:
		values = yaml.safe_load(stream) or {}

	containers = values.get("containers", {})
	storage = values.get("storage", {})
	for path_key in ["data_dir"]:
		path_value = storage.get(path_key)
		if path_value and not Path(path_value).is_absolute():
			storage[path_key] = str(ROOT_DIR / path_value)

	return Config(
		ROOT_DIR=ROOT_DIR,
		app=_section(AppConfig, values.get("app")),
		storage=_section(StorageConfig, storage),
		observation=_section(ObservationConfig, values.get("observation")),
		processing=_section(ProcessingConfig, values.get("processing")),
		containers=ContainersConfig(
			runner=containers.get("runner", "docker"),
			receive=_section(ContainerImageConfig, containers.get("receive")),
			process=_section(ContainerImageConfig, containers.get("process")),
			volume_name=containers.get("volume_name", "ska-sdp-data"),
			mount_path=containers.get("mount_path", "/data"),
		),
		quality_gate=_section(QualityGateConfig, values.get("quality_gate")),
		kubernetes=_section(KubernetesConfig, values.get("kubernetes")),
		logging=_section(LoggingConfig, values.get("logging")),
	)



config = load_config(ROOT_DIR / "config" / "settings.yaml")
