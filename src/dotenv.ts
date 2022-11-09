import dotenv from 'dotenv';

export function setup() {
  let path = '';

  if (process.env.NODE_ENV) {
    path += `.env.${process.env.NODE_ENV.toLowerCase()}`;
  } else {
    path += '.env';
  }

  console.log(`[dotenv]: Loading environment variables from ${path}`);

  dotenv.config({ path });
}

export default {
  setup,
}
