FROM node:20-bookworm

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .

# Install chromium dependencies & browser
RUN npx playwright install --with-deps chromium

ENV PORT=3000
EXPOSE 3000

CMD ["npm","start"]
