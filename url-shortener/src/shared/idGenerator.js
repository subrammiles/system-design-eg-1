// Distributed ID generation = Range Allocation + Base62 (+ scrambling).
//
//   Server requests ID range  ->  stores range locally  ->  generates IDs without DB calls
//
// 1. Each app server atomically reserves a block of IDs (e.g. 1000) from a
//    DynamoDB counter item (`ADD value :rangeSize`). DynamoDB has no
//    AUTO_INCREMENT, so this atomic counter is the "allocator".
// 2. IDs inside the block are handed out from memory -> one DB call per 1000 URLs.
//    If a server crashes, unused IDs are lost - acceptable, uniqueness matters,
//    not consecutiveness.
// 3. Sequential codes are guessable (short.ly/1, short.ly/2 ...). We scramble the
//    ID with a bijective permutation over [0, 62^L) before Base62 encoding, so
//    uniqueness is preserved but adjacent IDs give unrelated-looking codes.
import { UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { encode } from './base62.js';

// Affine permutation f(x) = (x * P + OFFSET) mod 62^L.
// Bijective because gcd(P, 62^L) = 1 (P is odd and not divisible by 31).
// P ~= space * golden ratio, so consecutive IDs land far apart in the code space
// (different leading characters), not just in the last few characters.
// Note: this is obfuscation, not cryptography - use a keyed cipher (e.g. FF1) if
// codes must be provably unguessable.
const OFFSET_SEED = 912_673_221_045n;

function pickMultiplier(space) {
  let p = (space * 618_033_988_749n) / 1_000_000_000_000n;
  while (p % 2n === 0n || p % 31n === 0n) p += 1n;
  return p;
}

function modInverse(a, m) {
  let [oldR, r] = [a % m, m];
  let [oldS, s] = [1n, 0n];
  while (r !== 0n) {
    const q = oldR / r;
    [oldR, r] = [r, oldR - q * r];
    [oldS, s] = [s, oldS - q * s];
  }
  if (oldR !== 1n) throw new Error('multiplier is not invertible modulo space size');
  return ((oldS % m) + m) % m;
}

export function makeScrambler(codeLength) {
  const space = 62n ** BigInt(codeLength);
  const P = pickMultiplier(space);
  const OFFSET = OFFSET_SEED % space;
  const pInv = modInverse(P, space);
  return {
    space,
    scramble: (id) => {
      const x = BigInt(id);
      if (x >= space) throw new RangeError(`ID space exhausted for length ${codeLength}`);
      return (x * P + OFFSET) % space;
    },
    unscramble: (y) => (((BigInt(y) - OFFSET) % space + space) * pInv) % space,
  };
}

export class RangeIdGenerator {
  /**
   * @param {object} opts
   * @param {() => Promise<bigint>} opts.reserveRange  atomically reserves `rangeSize` IDs, returns the new upper bound
   * @param {number} opts.rangeSize
   * @param {number} opts.codeLength
   */
  constructor({ reserveRange, rangeSize, codeLength }) {
    this.reserveRange = reserveRange;
    this.rangeSize = BigInt(rangeSize);
    this.codeLength = codeLength;
    this.scrambler = makeScrambler(codeLength);
    this.next = 0n;
    this.end = 0n; // exclusive
    this.pending = null; // in-flight reservation shared by concurrent callers
  }

  async nextId() {
    while (this.next >= this.end) {
      if (!this.pending) {
        this.pending = this.reserveRange()
          .then((upper) => {
            this.end = upper + 1n;
            this.next = upper - this.rangeSize + 1n;
          })
          .finally(() => {
            this.pending = null;
          });
      }
      await this.pending;
    }
    return this.next++;
  }

  async nextCode() {
    const id = await this.nextId();
    return encode(this.scrambler.scramble(id), this.codeLength);
  }
}

// DynamoDB-backed allocator: counters table, item { name: "url_id", value: N }.
export function dynamoRangeReserver(docClient, table, rangeSize, counterName = 'url_id') {
  return async () => {
    const res = await docClient.send(
      new UpdateCommand({
        TableName: table,
        Key: { name: counterName },
        UpdateExpression: 'ADD #v :inc',
        ExpressionAttributeNames: { '#v': 'value' },
        ExpressionAttributeValues: { ':inc': rangeSize },
        ReturnValues: 'UPDATED_NEW',
      }),
    );
    return BigInt(res.Attributes.value);
  };
}
