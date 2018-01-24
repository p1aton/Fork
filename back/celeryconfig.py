imports = ('tasks')
broker_url = 'amqp://'

from datetime import timedelta

beat_schedule = {
    'wex-parse': {
        'task': 'tasks.wex_parse',
        'schedule': timedelta(seconds=10),
    },
    'cex-parse': {
	'task': 'tasks.cex_parse',
	'schedule': timedelta(seconds=10),
    },
}
