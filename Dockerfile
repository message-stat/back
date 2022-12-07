ARG NODE_VERSION=16.18-alpine

FROM node:${NODE_VERSION}

WORKDIR /app

COPY . .

RUN npm install

RUN npm run build

CMD ["npm", "run", "docker"]
