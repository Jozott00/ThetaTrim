from typing import Any

from utils import utils


class Config:
  format: None | str = None
  filters: dict[str: dict[str: any]] = {}
  extract_audio: bool = False

  def __init__(self, config: list[dict[str, any]]):
    self.valid_formats = ['mp4', 'mov', 'avi']
    self.filter_operations = ["crop", "resize", "sepia", "brightness", "grayscale"]
    self.used_filters = set()

    for operation in config:

      for key in operation.keys():
        if key not in ['operation', 'opts']:
          raise KeyError(f"Invalid key '{key}' in configuration. Valid keys are 'operation' and 'opts'")

      op_type = operation.get('operation')
      op_opts = operation.get('opts')

      try:
        if op_type in self.filter_operations:
          self._check_filters(op_type, op_opts)
        elif op_type == 'format':
          self._check_format(op_opts)
        elif op_type == 'exaudio':
          self.extract_audio = True
        else:
          raise utils.ConfigError(f"Unsupported operation: '{op_type}'")
      except ValueError as e:
        raise utils.ConfigError(f"Invalid operation arguments for '{op_type}': '{op_opts}'\n", e)

  def _check_filters(self, op_type, op_opts):
    if op_type in self.used_filters:
      raise utils.ConfigError(f"Duplicate filter operation: '{op_type}'. Each filter operation can only be used once.")
    self.used_filters.add(op_type)

    if op_type == 'crop' or op_type == 'resize':
      opts_list = op_opts.split(' ')
      if len(opts_list) not in [2, 4]:
        raise utils.ConfigError(f"Invalid {op_type} options: {op_opts}")
      opts_dict = dict(zip(['width', 'height', 'x', 'y'][:len(opts_list)], map(int, opts_list)))
    elif op_type == 'brightness':
      opts_dict = {'value': float(op_opts)}
    else:
      opts_dict = {}

    self.filters[op_type] = opts_dict

  def _check_format(self, op_opts):
    format_opt = op_opts.strip()
    if format_opt not in self.valid_formats:
      raise utils.ConfigError(f"Configured format {format_opt} is not a valid format: {self.valid_formats}")

    self.format = format_opt


def get_job_config(job_table, job_id: str) -> Config:
  response = job_table.get_item(
    Key={
      'PK': f"JOB#{job_id}",
      'SK': "DATA"
    }
  )

  item = response.get('Item', None)

  if item is None:
    raise ValueError(f"Job information of {job_id} not found!")

  return Config(item['transformations'])
