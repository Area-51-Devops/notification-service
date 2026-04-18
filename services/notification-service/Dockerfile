FROM node:20-alpine
WORKDIR /usr/src/app

# Upgrade all OS packages to latest patched versions.
RUN apk update && apk upgrade --no-cache

# Context is ./services, so copy paths must be relative to the services/ directory.
COPY notification-service/package*.json ./
RUN (npm ci --omit=dev || npm install --omit=dev) && \
  npm cache clean --force && \
  rm -rf /usr/local/lib/node_modules/npm /usr/local/bin/npm /usr/local/bin/npx
COPY notification-service/src ./src
COPY shared ./shared

EXPOSE 3006
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
  CMD wget -qO- http://localhost:3006/health/startup || exit 1
CMD ["node", "src/index.js"]
