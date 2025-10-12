# multi-stage - installs deps in first stage (small final image)
FROM node:18-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --production

FROM node:18-alpine AS runtime
WORKDIR /app

# create non-root user for safety
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# copy only production deps
COPY --from=deps /app/node_modules ./node_modules
# copy app source
COPY . .

RUN chown -R appuser:appgroup /app
USER appuser

ENV NODE_ENV=production
EXPOSE 3000
CMD ["node", "server.js"]
