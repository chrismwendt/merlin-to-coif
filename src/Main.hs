module Main where

main :: IO ()
main = putStr "hello, world\n"

ghci :: IO ()
ghci = do
  putStrLn "Ready."

  return ()
