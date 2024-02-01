def get_jobid_from_key(key: str):
  return key.split("/")[0]


def get_extension_from_key(key: str):
  return key.rsplit(".")[1]


class InternalError(Exception):
  pass


class ApplicationError(Exception):
  pass


class FFmpegError(ApplicationError):
  pass


class ConfigError(ApplicationError):
  pass
