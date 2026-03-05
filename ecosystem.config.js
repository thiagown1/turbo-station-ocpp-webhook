module.exports = {
    apps: [
        {
            name: 'ocpp-webhook',
            script: '/var/www/turbo-station-ocpp-webhook/venv/bin/python3',
            args: 'webhook_worker.py',
            cwd: '/var/www/turbo-station-ocpp-webhook',
            interpreter: 'none',
            instances: 1,
            autorestart: true,
            watch: false,
            max_memory_restart: '500M',
            restart_delay: 2000,
            env: {
                ENVIRONMENT: 'production',
            },
            env_production: {
                NODE_ENV: 'production'
            },
            error_file: '/var/www/turbo-station-ocpp-webhook/logs/production/ocpp-webhook-error.log',
            out_file: '/var/www/turbo-station-ocpp-webhook/logs/production/ocpp-webhook-out.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
            merge_logs: true,
        },
        {
            name: 'ocpp-webhook_dev',
            script: '/var/www/turbo-station-ocpp-webhook/venv/bin/python3',
            args: 'webhook_worker.py',
            cwd: '/var/www/turbo-station-ocpp-webhook',
            interpreter: 'none',
            instances: 1,
            autorestart: true,
            watch: true,
            max_memory_restart: '500M',
            restart_delay: 2000,
            env: {
                ENVIRONMENT: 'development',
            },
            env_development: {
                NODE_ENV: 'development'
            },
            ignore_watch: [
                'logs',
                'venv',
                '.git',
                'node_modules',
                '__pycache__',
                '*.pyc'
            ],
            error_file: '/var/www/turbo-station-ocpp-webhook/logs/development/ocpp-webhook-dev-error.log',
            out_file: '/var/www/turbo-station-ocpp-webhook/logs/development/ocpp-webhook-dev-out.log',
            log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
            merge_logs: true,
        },
    ],
};
