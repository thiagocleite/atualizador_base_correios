using System;
using System.Security.Cryptography;

namespace VI.Anonimizator
{
    /// <summary>
    /// https://riptutorial.com/csharp/example/15470/complete-password-hashing-solution-using-pbkdf2
    /// https://cryptobook.nakov.com/mac-and-key-derivation/pbkdf2
    /// </summary>
    internal class PBKDF2
    {
        private const int HASH_BYTE_SIZE = 16;
        private const int PBKDF2_ITERATIONS = 4;
        private readonly byte[] _salt;

        public PBKDF2(string salt)
        {
            _salt = CreateSalt(salt);
        }

        public string GetHashBase64String(string value)
        {
            var hash = Encrypt(value, _salt);

            return Convert.ToBase64String(hash).Substring(0, 20);
        }

        private byte[] CreateSalt(string salt)
        {
            byte[] bytes = new byte[salt.Length * sizeof(char)];

            Buffer.BlockCopy(salt.ToCharArray(), 0, bytes, 0, bytes.Length);

            return bytes;
        }

        private byte[] Encrypt(string value, byte[] salt)
        {
            Rfc2898DeriveBytes pbkdf2 = new Rfc2898DeriveBytes(value, salt)
            {
                IterationCount = PBKDF2_ITERATIONS
            };

            return pbkdf2.GetBytes(HASH_BYTE_SIZE);
        }
    }
}
