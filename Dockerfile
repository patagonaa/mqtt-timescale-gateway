FROM node:22-alpine
ENV NODE_ENV="production"
WORKDIR /usr/src/app
COPY package.json package-lock.json ./
RUN npm install
COPY src/gateway.mjs .

CMD ["node", "./index.mjs"]
