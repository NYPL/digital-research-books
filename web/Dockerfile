FROM node:18-alpine AS base

# Install dependencies only when needed
FROM base AS deps
WORKDIR /app

# Install dependencies.
COPY package.json package-lock.json ./
RUN npm ci --cache .npm

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

ARG airtable_api_key
ARG NEW_RELIC_LICENSE_KEY
ARG NEW_RELIC_APP_NAME 
ARG NEXT_PUBLIC_ADOBE_ANALYTICS 
ARG APP_ENV

# Build the app!
ENV PATH /app/node_modules/.bin:$PATH
ENV PORT=3000 \
    NODE_ENV=production
ENV NEXT_PUBLIC_AIRTABLE_API_KEY $airtable_api_key
ENV NEW_RELIC_LICENSE_KEY $NEW_RELIC_LICENSE_KEY
ENV NEW_RELIC_APP_NAME $NEW_RELIC_APP_NAME
ENV NEXT_PUBLIC_ADOBE_ANALYTICS $NEXT_PUBLIC_ADOBE_ANALYTICS
ENV APP_ENV $APP_ENV

RUN npm run build

# RUNNER, copy all the files and run next
FROM base AS runner
WORKDIR /app

# We need to expose these ARG again, they are needed for browser monitor
ARG NEW_RELIC_APP_NAME
ARG NEW_RELIC_LICENSE_KEY
ENV NEW_RELIC_APP_NAME $NEW_RELIC_APP_NAME
ENV NEW_RELIC_LICENSE_KEY $NEW_RELIC_LICENSE_KEY

RUN addgroup --system --gid 1001 nodejs
RUN adduser --system --uid 1001 nextjs

# Set the correct permission for prerender cache
RUN mkdir .next
RUN chown nextjs:nodejs .next

COPY --from=builder /app/next.config.js ./
COPY --from=builder /app/public ./public

# Automatically leverage output traces to reduce image size 
# https://nextjs.org/docs/advanced-features/output-file-tracing
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static
COPY --from=builder --chown=nextjs:nodejs /app/newrelic.js ./newrelic.js

USER nextjs

EXPOSE $PORT

ENTRYPOINT ["node", "server.js"]

CMD ["r", "newrelic"]
