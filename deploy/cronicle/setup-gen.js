// Rewrites conf/setup.json, which `bin/storage-cli.js setup` reads once on a
// fresh volume: the admin password and the provisioning API key come from the
// environment, and the primary server group matches any hostname.
'use strict';
const crypto = require('crypto');
const fs = require('fs');
const bcrypt = require('/opt/cronicle/node_modules/bcryptjs');

const FILE = '/opt/cronicle/conf/setup.json';

function main() {
  const password = process.env.CRONICLE_ADMIN_PASSWORD || 'admin';
  const apiKey = process.env.CRONICLE_API_KEY || '';
  if (!/^\w{16,}$/.test(apiKey)) {
    throw new Error('CRONICLE_API_KEY must be at least 16 characters of [A-Za-z0-9_]');
  }
  const setup = JSON.parse(fs.readFileSync(FILE, 'utf8'));
  const now = Math.floor(Date.now() / 1000);
  for (const [op, key, obj] of setup.storage) {
    if (op === 'put' && key === 'users/admin') {
      obj.salt = crypto.randomBytes(32).toString('hex');
      obj.password = bcrypt.hashSync(password + obj.salt, 10); // pixl-server-user: bcrypt(password + salt)
    }
    if (op === 'listPush' && key === 'global/server_groups' && obj.id === 'maingrp') {
      obj.regexp = '.+';
    }
  }
  // Retention and VACUUM share logs/maintenance/etl.log: at most one runs at a
  // time (a blocked catch-up event retries the next minute).
  setup.storage.push(['listPush', 'global/categories', {
    id: 'maintenance',
    title: 'Maintenance',
    description: 'Postgres maintenance jobs; one at a time.',
    enabled: 1,
    max_children: 1,
    username: 'admin',
    created: now,
    modified: now,
  }]);
  setup.storage.push(['listPush', 'global/api_keys', {
    id: 'ketlprovision',
    title: 'etl-provision',
    description: 'Creates and updates the ETL events at container start.',
    key: apiKey,
    active: 1,
    username: 'admin',
    created: now,
    modified: now,
    privileges: { create_events: 1, edit_events: 1, delete_events: 0, run_events: 1, abort_events: 1, state_update: 0 },
  }]);
  fs.writeFileSync(FILE, JSON.stringify(setup, null, 2));
  console.log('setup-gen: wrote ' + FILE);
}

try { main(); } catch (e) { console.error('setup-gen: ' + e.message); process.exit(1); }
