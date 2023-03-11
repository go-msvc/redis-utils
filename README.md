# REDIS

2023-02-12
- Implemented most of what Philip did, but not health checks...
- Seems basic get/set is working in standalone - but need much more testing later
- For now wants to get KV to work, and get needs to get the data type back
        Think it will be bets to get with Tmpl and get that to work
        And normal Get() will return a best guess of the type as it does now

        So, next: test keyvalue.Store.GetTmpl() mechanism in REDIS

Later also test pubsub so one can use it for queued and may be also scheduled events...


Create a project docker-compose with all working components as they become workable/ready...
