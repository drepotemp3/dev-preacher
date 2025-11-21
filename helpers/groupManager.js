import { Account } from '../models/db.js';
import { fetchAccountGroups, leaveGroup } from './telegram.js';

/**
 * Fetch and update groups for all accounts
 */
export async function fetchAllAccountGroups() {
  const accounts = await Account.find({});
  
  console.log(`📡 Fetching groups for ${accounts.length} accounts...`);
  
  for (const account of accounts) {
    console.log(`  🔍 ${account.username || account.number}...`);
    
    const groups = await fetchAccountGroups(account.session);
    
    if (groups) {
      await Account.updateOne(
        { _id: account._id },
        { $set: { groups: groups } }
      );
      console.log(`     ✅ Updated ${groups.length} groups`);
    } else {
      console.log(`     ⚠️  Failed to fetch groups`);
    }
  }
}

/**
 * Handle duplicate group memberships (remove extra members)
 */
export async function handleDuplicateGroups() {
  const accounts = await Account.find({ admin: false });
  
  // Map groupId -> [accounts]
  const groupMap = new Map();
  
  for (const account of accounts) {
    for (const group of account.groups) {
      if (!groupMap.has(group.id)) {
        groupMap.set(group.id, []);
      }
      groupMap.get(group.id).push(account);
    }
  }
  
  // Find duplicates
  const duplicates = Array.from(groupMap.entries())
    .filter(([_, accs]) => accs.length > 1);
  
  if (duplicates.length === 0) {
    console.log('  ℹ️  No duplicate memberships');
    return;
  }
  
  console.log(`  ⚠️  Found ${duplicates.length} duplicate groups`);
  
  for (const [groupId, accountsInGroup] of duplicates) {
    // Keep one random account, remove others
    const shuffled = [...accountsInGroup].sort(() => Math.random() - 0.5);
    const toLeave = shuffled.slice(0, -1);
    
    for (const account of toLeave) {
      const success = await leaveGroup(account.session, groupId);
      
      if (success) {
        await Account.updateOne(
          { _id: account._id },
          { $pull: { groups: { id: groupId } } }
        );
        console.log(`     ✅ ${account.username || account.number} left group`);
      }
    }
  }
}

/**
 * Find groups where only admin is a member
 */
export async function findAdminOnlyGroups() {
  const adminAccount = await Account.findOne({ admin: true });
  
  if (!adminAccount) {
    return [];
  }
  
  const nonAdminAccounts = await Account.find({ admin: false });
  
  // Collect all non-admin group IDs
  const nonAdminGroupIds = new Set();
  for (const account of nonAdminAccounts) {
    for (const group of account.groups) {
      nonAdminGroupIds.add(group.id);
    }
  }
  
  // Find admin-only groups
  const adminOnlyGroups = adminAccount.groups.filter(
    group => !nonAdminGroupIds.has(group.id)
  );
  
  return adminOnlyGroups;
}