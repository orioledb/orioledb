# health-checker

Simple "hello world" program that sends the setup complete signal to Antithesis as a side effect of running.
The setup complete signal tells Antithesis the system is ready for fault injection and fuzzing.

It's only useful in the context of `config/docker-compose.yaml`, depending on all services to be ready. 
