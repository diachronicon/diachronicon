import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    BASEDIR = basedir

    SECRET_KEY = os.environ.get('SECRET_KEY') or 'constructicon-now-with-history-wow'

    SQLALCHEMY_DATABASE_URI_TEMPLATE = 'sqlite:///' + basedir + '{}'
    SQLALCHEMY_DATABASE_URI = (
        os.environ.get('DATABASE_URL')
        or 'sqlite:///' + os.path.join(basedir, 'diachronicon.db')
    )
    SQLALCHEMY_ECHO = os.environ.get('SQLALCHEMY_ECHO') == 'debug' or False
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    JINJA_OPTIONS = {
        'trim_blocks': True,
        'lstrip_blocks': True,
    }
    LOGGING_FILE = os.environ.get('FLASK_LOGGING_FILE') or 'logs/base.log'

    # ------------------------------------------------------------------
    # Semantic / vector search
    # ------------------------------------------------------------------
    # Model used to embed construction text fields.
    # 'paraphrase-multilingual-mpnet-base-v2' handles Russian well.
    EMBEDDING_MODEL = (
        os.environ.get('EMBEDDING_MODEL')
        or 'paraphrase-multilingual-mpnet-base-v2'
    )

    # ------------------------------------------------------------------
    # LLM integrations (annotation tool)
    # ------------------------------------------------------------------
    OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
    ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY')
    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY')

    # Default models used when the annotator does not specify one
    OPENAI_DEFAULT_MODEL = os.environ.get('OPENAI_DEFAULT_MODEL') or 'gpt-4o'
    ANTHROPIC_DEFAULT_MODEL = (
        os.environ.get('ANTHROPIC_DEFAULT_MODEL') or 'claude-sonnet-4-5-20251001'
    )
    GEMINI_DEFAULT_MODEL = (
        os.environ.get('GEMINI_DEFAULT_MODEL') or 'gemini-2.0-flash'
    )


class TestConfig(Config):
    TESTING = True
    SQLALCHEMY_ECHO = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    LOGGING_FILE = os.environ.get('FLASK_LOGGING_FILE') or 'logs/test.log'
    # Use an in-memory database for tests
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    # Disable CSRF for test client POSTs
    WTF_CSRF_ENABLED = False


loggingConfig = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
        }
    },
    'handlers': {
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default',
        },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': Config.LOGGING_FILE,
            'encoding': 'utf-8',
            'maxBytes': 8 * 2 ** 23,   # 8 MB
            'backupCount': 5,
            'formatter': 'default',
            'level': 'INFO',
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['wsgi', 'file'],
    },
}