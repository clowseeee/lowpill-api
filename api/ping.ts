export default async function handler(req: any, res: any) {
  if (req.method !== 'GET') return res.status(405).send('Method not allowed');
  res.status(200).send('pong');
}
