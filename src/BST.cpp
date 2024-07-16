#include<iostream>
#include<string>
#include<BST.h>
using namespace std;
typedef BST::Node Node;
void BST::Insert(const string& key, const string& value)
{
	if (_root == nullptr)//如果根为空，则创建根
	{
		_root = new Node(key, value);
        size = 1;
    	return;
	}
	Node* parent = nullptr;
	Node* cur = _root;
 	while (cur)
	{
		if (cur->_key < key)
		{
			parent = cur;
			cur = cur->_pRight;
		}
		else if (cur->_key > key)
		{
			parent = cur;
			cur = cur->_pLeft;
		}
		else//如果相等便不再插入
		{
            cur->_value = value;
			return;
		}
	}

	cur = new Node(key, value);
	//链接
	if (parent->_key > key)
	{
		parent->_pLeft = cur;
	}
	else
	{
		parent->_pRight = cur;
	}
    size++;
	return;
}
BST::Node* BST::Find(const string& key)
{
    Node* cur = _root;
    while (cur)
    {
        if (cur->_key > key)
        {
            cur = cur->_pLeft;
        }
        else if (cur->_key < key)
        {
            cur = cur->_pRight;
        }
        else
        {
            return cur;
        }
    }
    return nullptr;//虽然可以返回cur，但是返回nullptr更保险
}
 
bool BST::Erase(const string& key)
{
    Node* parent = nullptr;
    Node* cur = _root;

    while (cur)//安排好cur和parent
    {
        if (cur->_key > key)
        {
            parent = cur;
            cur = cur->_pLeft;
        }
        else if (cur->_key < key)
        {
            parent = cur;
            cur = cur->_pRight;
        }
        else
        {
            //1.左为空
            if (cur->_pLeft == nullptr)
            {
                if (cur == _root)
                {
                    _root = cur->_pRight;
                }
                else
                {
                    if (parent->_pLeft == cur)
                    {
                        parent->_pLeft = cur->_pRight;
                    }
                    else
                    {
                        parent->_pRight = cur->_pRight;
                    }
                }

                delete cur;
            }
            else if (cur->_pRight == nullptr)//2.右为空
            {
                if (cur == _root)
                {
                    _root = cur->_pLeft;
                }
                else
                {
                    if (parent->_pLeft == cur)
                    {
                        parent->_pLeft = cur->_pLeft;
                    }
                    else
                    {
                        parent->_pRight = cur->_pLeft;
                    }
                }

                delete cur;
            }
            else//3.左右孩子都存在，找左树最大节点或右树的最小节点
            {
                Node* pchild = cur;
                Node* child = cur->_pLeft;
                while (child->_pRight)
                {
                    pchild = child;
                    child = child->_pRight;
                }

                cur->_key = child->_key;
                cur->_value = child->_value;

                if (pchild->_pLeft == cur)//防止左右树只有一个节点无法进入循环
                {
                    pchild->_pLeft = child->_pLeft;
                }
                else
                {
                    pchild->_pRight = child->_pLeft;
                }

                delete child;
                size--;
            }
            return true;
        }
    }
    return false;
}
 
void BST::_InOrder(BST::Node* root)
{
    if (root == nullptr)
    {
        return;
    }

    _InOrder(root->_pLeft);
    cout << root->_key << ":" << root->_value << endl;
    _InOrder(root->_pRight);
}

void BST::InOrder()
{
    _InOrder(_root);
    cout << endl;
}

void BST::_deleteNode(BST::Node* node) {
    if (node) {
        _deleteNode(node->_pLeft);
        _deleteNode(node->_pRight);
        delete node;
    }
}

string BST::ser(BST::Node* root){
    if(root == nullptr){
        return "#";
    }
    return root->_key + ":" + root->_value + " " + ser(root->_pLeft) + " " + ser(root->_pRight);
}

string BST::serialize(){
    return ser(_root);
}

void BST::deserialize(string data){
    istringstream in(data);
    _root = deser(in);
}

Node *BST::deser(istringstream& in){
    if(_root != nullptr)
        _deleteNode(_root);
    string val;
    in >> val;
    if(val == "#"){
        return nullptr;
    }
    string key = val.substr(0, val.find(":"));
    string value = val.substr(val.find(":") + 1);
    BST::Node *root = new Node(key, value);
    root->_pLeft = deser(in);
    root->_pRight = deser(in);
    return root;
}

BST::~BST() {
    size = 0;
    _deleteNode(_root);
}

Node* BST::_getRoot() {
    return _root;
}