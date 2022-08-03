# https://github.com/exasol/docker-db/issues/45
#!/usr/bin/env bash
echo 'backup exasol conf'
docker-compose run \
--entrypoint /usr/bin/cp \
--rm exasol \
/exa/etc/EXAConf /exa/etc/EXAConf.bck

echo 'remove conf'
docker-compose run \
--entrypoint /usr/bin/rm \
--rm exasol \
/exa/etc/EXAConf

#run exasol
docker-compose up -d exasol &

echo 'waiting 10s to exasol initialize'
sleep 10

echo 'stopping exasol'
docker-compose stop exasol

#get new ip
NEW_IP=$(docker-compose run --entrypoint /usr/bin/cat --rm exasol /exa/etc/EXAConf | grep 'PrivateNet' | sed -s 's/\s\+//g; s/PrivateNet=//g')
echo 'new ip is: '$NEW_IP

echo 'revert backup'
docker-compose run \
--entrypoint /usr/bin/cp \
--rm exasol \
/exa/etc/EXAConf.bck /exa/etc/EXAConf

echo 'set new ip'
docker-compose run --rm exasol exaconf modify-node -n 11 -p $NEW_IP

echo 'drop exasol conf backup'
docker-compose run \
--entrypoint /usr/bin/rm \
--rm exasol \
/exa/etc/EXAConf.bck

echo 'now run exasol'