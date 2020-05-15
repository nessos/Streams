FROM eiriktsarpalis/dotnet-sdk-mono:3.1.201-buster

WORKDIR /app
COPY . .

CMD ./build.sh Bundle