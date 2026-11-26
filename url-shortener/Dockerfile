FROM node:22-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev && npm cache clean --force
COPY src ./src
COPY scripts ./scripts
USER node
# Which process to run is chosen by docker-compose (`command:`).
CMD ["node", "src/url-service/server.js"]
