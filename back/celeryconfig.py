imports = ('tasks')
broker_url = 'amqp://'

from datetime import timedelta

beat_schedule = {
    'usdt-parse': {
        'task': 'tasks.usdt_parse',
        'schedule': timedelta(seconds=60),
    },
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
    'btf-parse': {
	'task': 'tasks.btf_parse',
	'schedule': timedelta(seconds=10),
    },
    'btx-parse': {
	'task': 'tasks.btx_parse',
	'schedule': timedelta(seconds=10),
    },
    'bnn-parse': {
	'task': 'tasks.bnn_parse',
	'schedule': timedelta(seconds=10),
    },
    'profit-parse': {
	'task': 'tasks.profit_parse',
	'schedule': timedelta(seconds=10),
    },
}
