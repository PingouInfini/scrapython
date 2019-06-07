rm -rf ./docker/context/dockerdist/*
touch ./docker/context/dockerdist/README.md
mkdir -p ./docker/context/dockerdist/common/ && cp -Rf ./common/* ./docker/context/dockerdist/common/
cp -Rf ./scrapython.iml ./docker/context/dockerdist/
cp -Rf ./scrapython.py ./docker/context/dockerdist/
cp -Rf ./scrapy.cfg ./docker/context/dockerdist/
cp -Rf ./entrypoint.sh ./docker/context/dockerdist/
cp -Rf ./requirements.txt ./docker/context/

docker-compose -f ./docker/scrapython.yml build
