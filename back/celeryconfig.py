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
    'pol-parse': {
	'task': 'tasks.pol_parse',
	'schedule': timedelta(seconds=10),
    },
    'bnn-parse': {
	'task': 'tasks.bnn_parse',
	'schedule': timedelta(seconds=10),
    },
    'btx-parse': {
	'task': 'tasks.btx_parse',
	'schedule': timedelta(seconds=10),
    },
}
