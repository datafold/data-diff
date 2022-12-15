#!/bin/bash

if [ -n "$VERTICA_URI" ]
    then
        echo "Check Vertica DB running..."
        while true
        do
            if docker logs dd-vertica | tail -n 100 | grep -q -i "vertica is now running"
            then
               echo "Vertica DB is ready";
               break;
            else
               echo "Waiting for Vertica DB starting...";
               sleep 10;
            fi
        done
fi
