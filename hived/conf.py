import os

try:
    import project_conf
except ImportError:
    project_conf = None


def get_var(name, default=None):
    env_var = os.getenv(name)
    project_var = getattr(project_conf, name, None)
    return env_var or project_var or default


QUEUE_HOST = get_var('QUEUE_HOST', 'localhost')
QUEUE_USER = get_var('QUEUE_USER', 'guest')
QUEUE_PASSWORD = get_var('QUEUE_PASSWORD', 'guest')

TRACING_DISABLED = get_var('TRACING_DISABLED') == '1'

EXTERNAL_IP_URL = get_var('EXTERNAL_IP_URL')
