# Project 4: Locking (Part 2)

**A working implementation of Part 1 is required for Part 2. If you have not yet finished
[Part 1](proj4-part1-README.md), you should do so before continuing.**

## Overview

In this part, you will implement the middle layer (LockContext) and the declarative layer (in LockUtil).

The `concurrency` directory contains a partial implementation of a
lock context (`LockContext`), which you must complete in this part of the project.

## LockContext (multigranularity constraints)

The `LockContext` class represents a single resource in the hierarchy; this is
where all multigranularity operations (such as enforcing that you have the appropriate
intent locks before acquiring or performing lock escalation) are implemented.

You will need to implement the following methods of `LockContext`:
- `acquire`: this method performs an acquire via the underlying `LockManager` after
  ensuring that all multigranularity constraints are met. For example,
  if the transaction has IS(database) and requests X(table), the appropriate
  exception must be thrown (see comments above method).
  If a transaction has a SIX lock, then it is redundant for the transactior to have an IS/S lock on any descendant resource. Therefore, in our implementation, we prohibit acquiring an IS/S lock if an ancestor has SIX, and consider this to be an invalid request.
- `release`: this method performs a release via the underlying `LockManager` after
  ensuring that all multigranularity constraints will still be met after release. For example,
  if the transaction has X(table) and attempts to release IX(database), the appropriate
  exception must be thrown (see comments above method).
- `promote`: this method performs a lock promotion via the underlying `LockManager` after
  ensuring that all multigranularity constraints are met. For example,
  if the transaction has IS(database) and requests a promotion from S(table) to X(table), the appropriate
  exception must be thrown (see comments above method).
  In the special case of promotion to SIX (from IS/IX/S), you should simultaneously release all descendant locks of type S/IS, since we disallow having IS/S locks on descendants when a SIX lock is held. You should also disallow promotion to a SIX lock if an ancestor has SIX, because this would be redundant.

  *Note*: this does still allow for SIX locks to be held under a SIX lock, in
  the case of promoting an ancestor to SIX while a descendant holds SIX. This is
  redundant, but fixing it is both messy (have to swap all descendant SIX locks
  with IX locks) and pointless (you still hold a lock on the descendant
  anyways), so we just leave it as is.

- `escalate`: this method performs lock escalation up to the current level (see below for more
  details). Since interleaving of multiple `LockManager` calls by multiple transactions (running
  on different threads) is allowed, you must make sure to only use one mutating call to the
  `LockManager` and only request information about the current transaction from the `LockManager`
  (since information pertaining to any other transaction may change between the querying and
  the acquiring).
- `getExplicitLockType`: this method returns the type of the lock explicitly held at the current level. For example,
  if a transaction has X(db), `dbContext.getExplicitLockType(transaction)` should return X, but
  `tableContext.getExplicitLockType(transaction)` should return NL (no lock explicitly held).
- `getEfectiveLockType`: this method returns the type of the lock either implicitly or explicitly held at the current level. For example,
  if a transaction has X(db), `dbContext.getEfectiveLockType(transaction)` should return X, and
  `tableContext.getEfectiveLockType(transaction)` should *also* return X (since we implicitly have an X lock on every
  table due to explicitly having an X lock on the entire database). Note that since an intent lock does *not*
  implicitly grant lock-acquiring privileges to lower levels, if a transaction only has SIX(database),
  `tableContext.getEfectiveLockType(transaction)` should return S (not SIX), since the transaction
  implicitly has S on table via the SIX lock, but not the IX part of the SIX lock (which is only available at the
  database level). Note that it is possible for the explicit lock type to be one type, and the effective lock type
  to be a different lock type, specifically if there is an ancestor that has a SIX lock.

  You will also find it helpful to implement the helper methods we provide the structure for you (hasSIXAncestor, sisDescendants). While you do not need to use our helper methods, we provide nice documentation of what they should do which will be used in the main functions.

##### Hierarchy

The `LockContext` objects all share a single underlying `LockManager` object, which was
implemented in the previous step. The `parentContext` method returns the parent of the
current context (e.g. the lock context of the database is returned when `tableContext.parentContext()`
is called), and the `childContext` method returns the child lock context with the name passed in
(e.g. `tableContext.childContext(0L)` returns the context of page 0 of the table). There is
exactly one `LockContext` for each resource: calling `childContext` with the same parameters
multiple times returns the same object.

The provided code already initializes this tree of lock contexts for you. For performance reasons,
however, we do not create lock contexts for every page of a table immediately. Instead, we create them
as the corresponding `Page` objects are created. This means that the capacity of the lock context
(which we define to be the total number of children of the lock context) is not necessarily
the number of `LockContext`s we created via `childContext`.

##### Lock Escalation

Lock escalation is the process of going from many fine locks (locks at lower levels in the hierarchy)
to a single coarser lock (lock at a higher level). For example, we can escalate many page locks
that a transaction holds into a single lock at the table level.

We perform lock escalation through `LockContext#escalate`. A call to this method should be interpreted
as a request to escalate all locks on descendants (these are the fine locks) into one lock on the
context that `escalate` was called with (the coarse lock). The fine locks may be any mix of intent
and regular locks, but we limit the coarse lock to be either S or X.

For example, if we have the following locks: IX(database), SIX(table), X(page 1), X(page 2), X(page 4),
and call `tableContext.escalate(transaction)`, we should replace the page-level locks
with a single lock on the table that encompasses them:

![Diagram of before/after escalate, ending with IX(database), X(table)](images/proj4-escalate1.png)

Likewise, if we called `dbContext.escalate(transaction)`, we should replace the page-level locks  and
table-level locks with a single lock on the database that encompasses them:

![Diagram of before/after escalate, ending with X(database)](images/proj4-escalate2.png)

Note that escalating to an X lock always "works" in this regard: having a coarse X lock definitely
encompasses having a bunch of finer locks. However, this introduces other complications: if the transaction
previously held only finer S locks, it would not have the IX locks required to hold an X lock, and escalating to an
X reduces the amount of concurrency allowed unnecessarily. We therefore require that `escalate` only escalate
to the least permissive lock type (out of {S, X}) that still encompasses the replaced finer locks
(so if we only had IS/S locks, we should escalate to S, not X).

Also note that since we are only escalating to S or X, a transaction that only has IS(database) would
escalate to S(database). Though a transaction that only has IS(database) technically has no locks
at lower levels, the only point in keeping an intent lock at this level would be to acquire a normal
lock at a lower level, and the point in escalating is to avoid having locks at a lower level. Therefore,
we don't allow escalating to intent locks (IS/IX/SIX).

## LockUtil (declarative constructs)

The LockContext class enforces multigranularity constraints for us, but it's a bit
cumbersome to use in our database: wherever we want to request some locks, we have to
handle requesting the appropriate intent locks, etc.

To simplify integrating locking into our codebase (the second half of this part), we define the
`ensureSufficientLockHeld` method. This method is used like a declarative statement. For example,
let's say we have some code that reads an entire table. To add locking, we can do:

```java
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);

// any code that reads the table here
```

After the `ensureSufficientLockHeld` line, we can assume that the current transaction (the transaction returned by
`Transaction.getTransaction()`) has permission
to read the resource represented by `tableContext`, as well as any children (all the pages).

We can call it several times in a row:

```java
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);

// any code that reads the table here
```

or write several statements in any order:


```java
LockUtil.ensureSufficientLockHeld(pageContext, LockType.S);
LockUtil.ensureSufficientLockHeld(tableContext, LockType.S);
LockUtil.ensureSufficientLockHeld(pageContext, LockType.S);

// any code that reads the table here
```

and no errors should be thrown, and at the end of the calls, we should be able to read all of
the table.

Note that the caller does not care exactly which locks the transaction actually has: if we
gave the transaction an X lock on the database, the transaction would indeed have permission to
read all of the table. But this doesn't allow for much concurrency (and actually enforces
a serial schedule if used with 2PL), so we additionally stipulate that `ensureSufficientLockHeld`
should grant as little additional permission as possible: if an S lock suffices, we should
have the transaction acquire an S lock, not an X lock, but if the transaction already has an X lock,
we should leave it alone (`ensureSufficientLockHeld` should never reduce the permissions
a transaction has; it should always let the transaction do at least as much as it used to, before
the call).

We suggest breaking up the logic of this method into two phases: ensuring that we have the appropriate
locks on ancestors, and acquiring the lock on the resource. You will need to promote in some cases,
and escalate in some cases (these cases are not mutually exclusive).

#### Additional Notes

After this, you should pass all the tests we have provided to you under `database.concurrency.*`.

You are strongly encouraged to test your code further with your own tests before continuing to the
next part (see the Testing section for instructions on how). Although you will not be graded
on testing, the provided tests are in no sense comprehensive, and bugs in this section **will**
cause additional difficulties in implementing and testing the next part of the project (this
was the case for very many students in past semesters).

Note that you may **not** modify the signature of any methods or classes that we
provide to you, but you're free to add helper methods. Also, you should only modify code in
the `concurrency` directory for this section.

