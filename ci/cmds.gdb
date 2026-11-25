thread apply all bt full
eval "p *((LWLockHandle (*) [%u]) held_lwlocks)", num_held_lwlocks
eval "p *((MyLockedPage (*) [%u]) myLockedPages)", numberOfMyLockedPages
up 99999
set $i=0
set $end=argc
while ($i < $end)
p argv[$i++]
end
frame function o_btree_iterator_fetch
p *it
p cmp
p result
p it->prevTuple
p *desc
quit